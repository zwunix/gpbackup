package end_to_end_compatibility_test

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"

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

var restoreCluster *cluster.Cluster

var unpacked_artifacts_dir string
var pluginDir = "/tmp/plugin_dest"

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Compatibility Suite")
}

var _ = Describe("backup end to end integration tests", func() {
	var restoreConn *dbconn.DBConn
	var backupHelperPath, restoreHelperPath, gprestorePath, pluginConfigPath string

	BeforeSuite(func() {
		pluginConfigPath =
			fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml",
				os.Getenv("HOME"))
		var err error
		testhelper.SetupTestLogger()
		exec.Command("dropdb", "restoredb").Run()

		err = exec.Command("createdb", "restoredb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create restoredb: %v", err))
		}
		restoreConn = testutils.SetupTestDbConn("restoredb")
		_, backupHelperPath, gprestorePath = buildAndInstallBinaries()
		restoreHelperPath = backupHelperPath

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
		segConfig := cluster.MustGetSegmentConfiguration(restoreConn)
		restoreCluster = cluster.NewCluster(segConfig)

		untargz(cwd + "/artifacts/5.x/1.7.1/gpbackup-1.7.1-artifacts.tar.gz")
		err = os.Chdir(cwd)
		if err != nil {
			Fail("cannot change to directory: " + unpacked_artifacts_dir)
		}

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
		pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
		copyPluginToAllHosts(restoreConn, pluginExecutablePath)
	})
	AfterSuite(func() {
		if restoreConn != nil {
			restoreConn.Close()
		}
		gexec.CleanupBuildArtifacts()
		err := exec.Command("dropdb", "restoredb").Run()
		if err != nil {
			fmt.Printf("Could not drop restoredb: %v\n", err)
		}
		// added to clean up artifact untarring
		_ = os.RemoveAll(unpacked_artifacts_dir)
	})

	BeforeEach(func() {
		skipIfOldBackupVersionBefore("1.7.0")
		// for now, limit restoring to 5x (archive is from 5x)
		if !(restoreConn.Version.AtLeast("5.0.0") &&
			restoreConn.Version.Before("6.0.0")) {
			Fail("cannot run compat with restore destination != 5x")
		}

		err := os.RemoveAll(pluginDir)
		if err != nil {
			Fail("cannot delete plugin directory for untarring backup: " + pluginDir)
		}
		err = os.MkdirAll(pluginDir, 0777)
		if err != nil {
			Fail("cannot create plugin directory for untarring backup: " + pluginDir)
		}
	})
	AfterEach(func() {
		testhelper.AssertQueryRuns(restoreConn, "DROP TABLE sales;")
		testhelper.AssertQueryRuns(restoreConn, "DROP TABLE foo;")
		_ = os.RemoveAll(pluginDir)
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		Describe("Single data file with a plugin", func() {
			It("runs gpbackup and gprestore with plugin, single-data-file, and no-compression", func() {
				timestamp := unpackBackupArtifact(`-plugin_single-data-file_no-compression\.tar\.gz`)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				// fails - the artifact is taken of the end_to_end DB (36 rels) rather than sample_data.sql
				assertRelationsCreated(restoreConn, 14)
				assertArtifactsCleaned(restoreConn, timestamp)
			})
			It("runs gpbackup and gprestore with plugin and single-data-file", func() {
				timestamp := unpackBackupArtifact(`-plugin_single-data-file\.tar\.gz`)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 14)
				assertArtifactsCleaned(restoreConn, timestamp)
			})
			It("runs gpbackup and gprestore with plugin and metadata-only", func() {
				timestamp := unpackBackupArtifact(`-plugin_metadata-only\.tar\.gz`)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 14)
				assertArtifactsCleaned(restoreConn, timestamp)
			})

		})
		Describe("Multi-file Plugin", func() {
			It("runs gpbackup and gprestore with plugin and no-compression", func() {
				timestamp := unpackBackupArtifact(`-plugin_no-compression\.tar\.gz`)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 14)

				os.RemoveAll(pluginDir)
			})
			It("runs gpbackup and gprestore with plugin and compression", func() {
				timestamp := unpackBackupArtifact(`-plugin\.tar\.gz`)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 14)

				os.RemoveAll(pluginDir)
			})
		})
		Describe("Incremental with a plugin", func() {
			It("Restores from an incremental backup based on a from-timestamp incremental", func() {
				fullBackupTimestamp := unpackBackupArtifact(`-plugin_single-data-file_leaf-partition-data\.tar\.gz`)

				incremental1Timestamp := unpackBackupArtifact(`-plugin_incremental_from-timestamp_\d{14}_single-data-file_leaf-partition-data\.tar\.gz`)

				incremental2Timestamp := unpackBackupArtifact(`-plugin_incremental_single-data-file_leaf-partition-data\.tar\.gz`)

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 14)
				assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
				assertArtifactsCleaned(restoreConn, incremental1Timestamp)
				assertArtifactsCleaned(restoreConn, incremental2Timestamp)
			})
		})
	})
})

// This function is run automatically by ginkgo before any tests are run.
func init() {
	flag.StringVar(&custom_backup_dir, "custom_backup_dir", "/tmp", "custom_backup_flag for testing against a configurable directory")
}

func gprestore(gprestorePath string, restoreHelperPath string, timestamp string, args ...string) []byte {
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

	fpInfo := backup_filepath.NewFilePathInfo(restoreCluster, "", timestamp, backup_filepath.GetSegPrefix(conn))
	description := "Checking if helper files are cleaned up properly"
	cleanupFunc := func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)

		return fmt.Sprintf("! ls %s && ! ls %s && ! ls %s && ! ls %s*", errorFile, oidFile, scriptFile, pipeFile)
	}
	remoteOutput := restoreCluster.GenerateAndExecuteCommand(description, cleanupFunc, cluster.ON_SEGMENTS_AND_MASTER)
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
	fpInfo := backup_filepath.NewFilePathInfo(restoreCluster, "", timestamp, backup_filepath.GetSegPrefix(conn))
	remoteOutput := restoreCluster.GenerateAndExecuteCommand(fmt.Sprintf("Removing backups on all segments for "+
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

func createGlobalObjects(conn *dbconn.DBConn) {
	if conn.Version.Before("6") {
		testhelper.AssertQueryRuns(conn, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
	} else {
		testhelper.AssertQueryRuns(conn, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir';")
	}
	testhelper.AssertQueryRuns(conn, "CREATE RESOURCE QUEUE test_queue WITH (ACTIVE_STATEMENTS=5);")
	testhelper.AssertQueryRuns(conn, "CREATE ROLE global_role RESOURCE QUEUE test_queue;")
	testhelper.AssertQueryRuns(conn, "CREATE ROLE testrole;")
	testhelper.AssertQueryRuns(conn, "GRANT testrole TO global_role;")
	testhelper.AssertQueryRuns(conn, "CREATE DATABASE global_db TABLESPACE test_tablespace;")
	testhelper.AssertQueryRuns(conn, "ALTER DATABASE global_db OWNER TO global_role;")
	testhelper.AssertQueryRuns(conn, "ALTER ROLE global_role SET search_path TO public,pg_catalog;")
	if conn.Version.AtLeast("5") {
		testhelper.AssertQueryRuns(conn, "CREATE RESOURCE GROUP test_group WITH (CPU_RATE_LIMIT=1, MEMORY_LIMIT=1);")
		testhelper.AssertQueryRuns(conn, "ALTER ROLE global_role RESOURCE GROUP test_group;")
	}
}

func dropGlobalObjects(conn *dbconn.DBConn, dbExists bool) {
	if dbExists {
		testhelper.AssertQueryRuns(conn, "DROP DATABASE global_db;")
	}
	testhelper.AssertQueryRuns(conn, "DROP TABLESPACE test_tablespace;")
	testhelper.AssertQueryRuns(conn, "DROP ROLE global_role;")
	testhelper.AssertQueryRuns(conn, "DROP ROLE testrole;")
	testhelper.AssertQueryRuns(conn, "DROP RESOURCE QUEUE test_queue;")
	if conn.Version.AtLeast("5") {
		testhelper.AssertQueryRuns(conn, "DROP RESOURCE GROUP test_group;")
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
			//fmt.Println("Creating directory :", filename)
			err = os.MkdirAll(filename, os.FileMode(header.Mode)) // or use 0755 if you prefer
			if err != nil {
				fmt.Println("cannot create directory", err)
				os.Exit(1)
			}
		case tar.TypeReg:
			// handle normal file
			//fmt.Println("Untarring :", filename)
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

func unpackBackupArtifact(regex_suffix string) string {
	files, err := ioutil.ReadDir(unpacked_artifacts_dir)
	gplog.FatalOnError(err)

	r, _ := regexp.Compile(fmt.Sprintf(`^(\d{14})%s$`, regex_suffix))

	var filename = ""
	for _, f := range files {
		if r.Match([]byte(f.Name())) {
			filename = f.Name()
			break
		}
	}
	if filename == "" {
		gplog.FatalOnError(errors.Errorf("cannot find filename that matches regexp: %s", regex_suffix))
	}
	timestamp := filename[:14]
	err = os.Chdir(pluginDir)
	if err != nil {
		Fail("cannot change to plugin directory: " + pluginDir)
	}
	untargz(unpacked_artifacts_dir + "/" + filename)

	return timestamp
}
