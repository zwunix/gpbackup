package end_to_end_compatibility_test

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

/* The backup directory must be unique per test. There is test flakiness
 * against Data Domain Boost mounted file systems due to how it handles
 * directory deletion/creation.
 */
var custom_backup_dir string

var useOldBackupVersion bool
var oldBackupSemVer semver.Version

var backupCluster *cluster.Cluster

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Compatibility Suite")
}

var _ = Describe("backup end to end integration tests", func() {
	const (
		GPDB4_OBJECTS    = "../end_to_end/gpdb4_objects.sql"
		GPDB5_OBJECTS    = "../end_to_end/gpdb5_objects.sql"
		GPDB6_OBJECTS    = "../end_to_end/gpdb6_objects.sql"
		TEST_TABLES_DDL  = "../end_to_end/test_tables_ddl.sql"
		TEST_TABLES_DATA = "../end_to_end/test_tables_data.sql"
	)

	var backupConn, restoreConn *dbconn.DBConn
	var gpbackupPath, backupHelperPath, restoreHelperPath, gprestorePath, pluginConfigPath string
	var unpacked_artifacts_dir string

	BeforeSuite(func() {

		// This is used to run tests from an older gpbackup version to gprestore latest
		useOldBackupVersion = os.Getenv("OLD_BACKUP_VERSION") != ""
		pluginConfigPath =
			fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml",
				os.Getenv("HOME"))
		var err error
		testhelper.SetupTestLogger()
		exec.Command("dropdb", "testdb").Run()
		exec.Command("dropdb", "restoredb").Run()

		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create testdb: %v", err))
		}
		err = exec.Command("createdb", "restoredb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create restoredb: %v", err))
		}
		backupConn = testutils.SetupTestDbConn("testdb")
		restoreConn = testutils.SetupTestDbConn("restoredb")
		testutils.ExecuteSQLFile(backupConn, TEST_TABLES_DDL)
		testutils.ExecuteSQLFile(backupConn, TEST_TABLES_DATA)
		if useOldBackupVersion {
			oldBackupSemVer = semver.MustParse(os.Getenv("OLD_BACKUP_VERSION"))
			_, restoreHelperPath, gprestorePath = buildAndInstallBinaries()
			gpbackupPath, backupHelperPath = buildOldBinaries(os.Getenv("OLD_BACKUP_VERSION"))
		} else {
			gpbackupPath, backupHelperPath, gprestorePath = buildAndInstallBinaries()
			restoreHelperPath = backupHelperPath
		}
		segConfig := cluster.MustGetSegmentConfiguration(backupConn)
		backupCluster = cluster.NewCluster(segConfig)

		if backupConn.Version.Before("6") {
			testutils.SetupTestFilespace(backupConn, backupCluster)
		} else {
			remoteOutput := backupCluster.GenerateAndExecuteCommand("Creating filespace test directories on all hosts", func(contentID int) string {
				return fmt.Sprintf("mkdir -p /tmp/test_dir && mkdir -p /tmp/test_dir1 && mkdir -p /tmp/test_dir2")
			}, cluster.ON_HOSTS_AND_MASTER)
			if remoteOutput.NumErrors != 0 {
				Fail("Could not create filespace test directory on 1 or more hosts")
			}
		}

		cwd, _ := os.Getwd()
		unpacked_artifacts_dir = os.TempDir() + "/unpacked_artifacts"
		err = os.MkdirAll(unpacked_artifacts_dir, 0777)
		if err != nil {
			Fail("cannot create directory for unpacking: " + unpacked_artifacts_dir)
		}
		err = os.Chdir(unpacked_artifacts_dir)
		if err != nil {
			Fail("cannot change to temporary directory: " + unpacked_artifacts_dir)
		}

		untargz(cwd + "/artifacts/5.x/1.7.1/gpbackup-1.7.1-artifacts.tar.gz")
		err = os.Chdir(cwd)
		if err != nil {
			Fail("cannot change to directory: " + unpacked_artifacts_dir)
		}
	})
	AfterSuite(func() {
		if backupConn.Version.Before("6") {
			testutils.DestroyTestFilespace(backupConn)
		} else {
			remoteOutput := backupCluster.GenerateAndExecuteCommand("Removing /tmp/test_dir* directories on all hosts", func(contentID int) string {
				return fmt.Sprintf("rm -rf /tmp/test_dir*")
			}, cluster.ON_HOSTS_AND_MASTER)
			if remoteOutput.NumErrors != 0 {
				Fail("Could not remove /tmp/testdir* directories on 1 or more hosts")
			}
		}
		if backupConn != nil {
			backupConn.Close()
		}
		if restoreConn != nil {
			restoreConn.Close()
		}
		gexec.CleanupBuildArtifacts()
		err := exec.Command("dropdb", "testdb").Run()
		if err != nil {
			fmt.Printf("Could not drop testdb: %v\n", err)
		}
		err = exec.Command("dropdb", "restoredb").Run()
		if err != nil {
			fmt.Printf("Could not drop restoredb: %v\n", err)
		}
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		var publicSchemaTupleCounts, schema2TupleCounts map[string]int

		BeforeEach(func() {
			testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
			publicSchemaTupleCounts = map[string]int{
				"public.foo":   40000,
				"public.holds": 50000,
				"public.sales": 13,
			}
			schema2TupleCounts = map[string]int{
				"schema2.returns": 6,
				"schema2.foo2":    0,
				"schema2.foo3":    100,
				"schema2.ao1":     1000,
				"schema2.ao2":     1000,
			}
		})
		Describe("Single data file", func() {
			Context("with plugin", func() {
				BeforeEach(func() {
					skipIfOldBackupVersionBefore("1.7.0")
				})
				FIt("runs gpbackup and gprestore with plugin, single-data-file, and no-compression", func() {

					// for now, limit restoring to 5x (archive is from 5x)
					if !(restoreConn.Version.AtLeast("5.0.0") &&
						restoreConn.Version.Before("6.0.0")) {
						Fail("cannot run compat with restore destination != 5x")
					}

					// role "pivotal" is required
					username := operating.System.Getenv("PGUSER")
					if username == "" {
						currentUser, _ := operating.System.CurrentUser()
						username = currentUser.Username
					}
					if username != "pivotal" {
						Fail(`In destination database, please create a greenplum user "pivotal" and set PGUSER to "pivotal"`)
					}

					const timestamp = "20190104163445"
					const artifact = "artifacts/5.x/1.7.1/20190104163445-plugin_no_compress_single-data-file_backup.tar.gz"
					cwd, _ := os.Getwd()
					tempdir := os.TempDir()
					err := os.Chdir(tempdir)
					if err != nil {
						Fail("cannot change to temporary directory: " + tempdir)
					}

					untargz(cwd + "/" + artifact)

					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					//timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--no-compression", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)

					_ = os.RemoveAll(tempdir)
				})
				It("runs gpbackup and gprestore with plugin and single-data-file", func() {
					pluginDir := "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(pluginDir)
				})
				It("runs gpbackup and gprestore with plugin and metadata-only", func() {
					pluginDir := "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(pluginDir)
				})
			})
		})
		Describe("Multi-file Plugin", func() {
			It("runs gpbackup and gprestore with plugin and no-compression", func() {
				skipIfOldBackupVersionBefore("1.7.0")
				pluginDir := "/tmp/plugin_dest"
				pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--no-compression", "--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)

				os.RemoveAll(pluginDir)
			})
			It("runs gpbackup and gprestore with plugin and compression", func() {
				skipIfOldBackupVersionBefore("1.7.0")
				pluginDir := "/tmp/plugin_dest"
				pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)

				os.RemoveAll(pluginDir)
			})
		})
		Describe("Incremental", func() {
			BeforeEach(func() {
				skipIfOldBackupVersionBefore("1.7.0")
			})
			Context("With a plugin", func() {
				var pluginDir string
				BeforeEach(func() {
					pluginDir = "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)
				})
				AfterEach(func() {
					os.RemoveAll(pluginDir)
				})
				It("Restores from an incremental backup based on a from-timestamp incremental", func() {
					fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--single-data-file", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, fullBackupTimestamp)
					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")

					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
					incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--single-data-file", "--from-timestamp",
						fullBackupTimestamp, "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, incremental1Timestamp)

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--single-data-file", "--plugin-config",
						pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, incremental2Timestamp)

					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
						"--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
					assertArtifactsCleaned(restoreConn, incremental1Timestamp)
					assertArtifactsCleaned(restoreConn, incremental2Timestamp)
				})
			})
		})
		It("runs example_plugin.sh with plugin_test_bench", func() {
			if useOldBackupVersion {
				Skip("This test is only needed for the latest backup version")
			}
			pluginsDir := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins", os.Getenv("HOME"))
			copyPluginToAllHosts(backupConn, fmt.Sprintf("%s/example_plugin.sh", pluginsDir))
			command := exec.Command("bash", "-c", fmt.Sprintf("%s/plugin_test_bench.sh %s/example_plugin.sh %s/example_plugin_config.yaml", pluginsDir, pluginsDir, pluginsDir))
			mustRunCommand(command)

			os.RemoveAll("/tmp/plugin_dest")
		})
	})
})

// This function is run automatically by ginkgo before any tests are run.
func init() {
	flag.StringVar(&custom_backup_dir, "custom_backup_dir", "/tmp", "custom_backup_flag for testing against a configurable directory")
}

/* This function is a helper function to execute gpbackup and return a session
 * to allow checking its output.
 */
func gpbackup(gpbackupPath string, backupHelperPath string, args ...string) string {
	if useOldBackupVersion {
		os.Chdir("..")
		command := exec.Command("make", "install_helper", fmt.Sprintf("helper_path=%s", backupHelperPath))
		mustRunCommand(command)
		os.Chdir("end_to_end_compatibility")
	}
	args = append([]string{"--verbose", "--dbname", "testdb"}, args...)
	command := exec.Command(gpbackupPath, args...)
	output := mustRunCommand(command)
	r := regexp.MustCompile(`Backup Timestamp = (\d{14})`)
	return r.FindStringSubmatch(fmt.Sprintf("%s", output))[1]
}

func gprestore(gprestorePath string, restoreHelperPath string, timestamp string, args ...string) []byte {
	if useOldBackupVersion {
		os.Chdir("..")
		command := exec.Command("make", "install_helper", fmt.Sprintf("helper_path=%s", restoreHelperPath))
		mustRunCommand(command)
		os.Chdir("end_to_end_compatibility")
	}
	args = append([]string{"--verbose", "--timestamp", timestamp}, args...)
	command := exec.Command(gprestorePath, args...)
	output := mustRunCommand(command)
	return output
}

func buildAndInstallBinaries() (string, string, string) {
	os.Chdir("..")
	command := exec.Command("make", "build")
	mustRunCommand(command)
	os.Chdir("end_to_end_compatibility")
	binDir := fmt.Sprintf("%s/go/bin", operating.System.Getenv("HOME"))
	return fmt.Sprintf("%s/gpbackup", binDir), fmt.Sprintf("%s/gpbackup_helper", binDir), fmt.Sprintf("%s/gprestore", binDir)
}

func buildOldBinaries(version string) (string, string) {
	os.Chdir("..")
	command := exec.Command("git", "checkout", version, "-f")
	mustRunCommand(command)
	command = exec.Command("dep", "ensure")
	mustRunCommand(command)
	gpbackupOldPath, err := gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup", "-ldflags", fmt.Sprintf("-X github.com/greenplum-db/gpbackup/backup.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	gpbackupHelperOldPath, err := gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup_helper", "-ldflags", fmt.Sprintf("-X github.com/greenplum-db/gpbackup/helper.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	command = exec.Command("git", "checkout", "-", "-f")
	mustRunCommand(command)
	command = exec.Command("dep", "ensure")
	mustRunCommand(command)
	os.Chdir("end_to_end_compatibility")
	return gpbackupOldPath, gpbackupHelperOldPath
}

func assertDataRestored(conn *dbconn.DBConn, tableToTupleCount map[string]int) {
	for name, numTuples := range tableToTupleCount {
		tupleCount := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT count(*) AS string from %s", name))
		Expect(tupleCount).To(Equal(strconv.Itoa(numTuples)))
	}
}

func assertRelationsCreated(conn *dbconn.DBConn, numTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('S','v','r') AND n.nspname IN ('public', 'schema2');`
	tableCount := dbconn.MustSelectString(conn, countQuery)
	Expect(tableCount).To(Equal(strconv.Itoa(numTables)))
}

func assertArtifactsCleaned(conn *dbconn.DBConn, timestamp string) {
	cmdStr := fmt.Sprintf("ps -ef | grep -v grep | grep -E gpbackup_helper.*%s || true", timestamp)
	output := mustRunCommand(exec.Command("bash", "-c", cmdStr))
	Eventually(func() string { return strings.TrimSpace(string(output)) }, 5*time.Second, 100*time.Millisecond).Should(Equal(""))

	fpInfo := backup_filepath.NewFilePathInfo(backupCluster, "", timestamp, backup_filepath.GetSegPrefix(conn))
	description := "Checking if helper files are cleaned up properly"
	cleanupFunc := func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)

		return fmt.Sprintf("! ls %s && ! ls %s && ! ls %s && ! ls %s*", errorFile, oidFile, scriptFile, pipeFile)
	}
	remoteOutput := backupCluster.GenerateAndExecuteCommand(description, cleanupFunc, cluster.ON_SEGMENTS_AND_MASTER)
	if remoteOutput.NumErrors != 0 {
		Fail(fmt.Sprintf("Helper files found for timestamp %s", timestamp))
	}
}

func mustRunCommand(cmd *exec.Cmd) []byte {
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	return output
}

func copyPluginToAllHosts(conn *dbconn.DBConn, pluginPath string) {
	hostnameQuery := `SELECT DISTINCT hostname AS string FROM gp_segment_configuration WHERE content != -1`
	hostnames := dbconn.MustSelectStringSlice(conn, hostnameQuery)
	pluginDir, _ := filepath.Split(pluginPath)
	for _, hostname := range hostnames {
		command := exec.Command("ssh", hostname, fmt.Sprintf("mkdir -p %s", pluginDir))
		mustRunCommand(command)
		command = exec.Command("scp", pluginPath, fmt.Sprintf("%s:%s", hostname, pluginPath))
		mustRunCommand(command)
	}
}

func forceMetadataFileDownloadFromPlugin(conn *dbconn.DBConn, timestamp string) {
	fpInfo := backup_filepath.NewFilePathInfo(backupCluster, "", timestamp, backup_filepath.GetSegPrefix(conn))
	remoteOutput := backupCluster.GenerateAndExecuteCommand(fmt.Sprintf("Removing backups on all segments for "+
		"timestamp %s", timestamp), func(contentID int) string {
		return fmt.Sprintf("rm -rf %s", fpInfo.GetDirForContent(contentID))
	}, cluster.ON_SEGMENTS_AND_MASTER)
	if remoteOutput.NumErrors != 0 {
		Fail(fmt.Sprintf("Failed to remove backup directory for timestamp %s", timestamp))
	}
}

func skipIfOldBackupVersionBefore(version string) {
	if useOldBackupVersion && oldBackupSemVer.LT(semver.MustParse(version)) {
		Skip(fmt.Sprintf("Feature not supported in gpbackup %s", oldBackupSemVer))
	}
}

// source: https://socketloop.com/tutorials/golang-untar-or-extract-tar-ball-archive-example
func untargz(source string) {
	sourcefile := source
	if sourcefile == "" {
		fmt.Println("Usage : go-untar sourcefile.tar")
		os.Exit(1)
	}
	file, err := os.Open(sourcefile)
	if err != nil {
		fmt.Println("cannot open file to be untarred: ", err)
		os.Exit(1)
	}
	defer file.Close()
	var fileReader io.ReadCloser = file
	// just in case we are reading a tar.gz file, add a filter to handle gzipped file
	if strings.HasSuffix(sourcefile, ".gz") {
		if fileReader, err = gzip.NewReader(file); err != nil {
			fmt.Println("cannot read tarball", err)
			os.Exit(1)
		}
		defer fileReader.Close()
	}
	tarBallReader := tar.NewReader(fileReader)
	// Extracting tarred files
	for {
		header, err := tarBallReader.Next()
		if err != nil {
			if err == io.EOF {
				break // the happy path exit
			}
			fmt.Println("cannot read next tarball item", err)
			os.Exit(1)
		}
		// get the individual filename and extract to the current directory
		filename := header.Name
		switch header.Typeflag {
		case tar.TypeDir:
			// handle directory
			fmt.Println("Creating directory :", filename)
			err = os.MkdirAll(filename, os.FileMode(header.Mode)) // or use 0755 if you prefer
			if err != nil {
				fmt.Println("cannot create directory", err)
				os.Exit(1)
			}
		case tar.TypeReg:
			// handle normal file
			fmt.Println("Untarring :", filename)
			writer, err := os.Create(filename)
			if err != nil {
				fmt.Println("cannot create untarred file", err)
				os.Exit(1)
			}
			_, err = io.Copy(writer, tarBallReader)
			if err != nil {
				fmt.Println("cannot copy untarred file", err)
				os.Exit(1)
			}
			err = os.Chmod(filename, os.FileMode(header.Mode))
			if err != nil {
				fmt.Println("cannot chmod untarred file", err)
				os.Exit(1)
			}
			_ = writer.Close()
		default:
			fmt.Printf("Unable to untar type : %c in file %s", header.Typeflag, filename)
		}
	}
}
