package testspanner

import (
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5"
)

var (
	targets = map[testing.TB]string{}
)

const (
	containerNamePrefix = "buildbuddy-test-spanner-"
)

// GetOrStart starts a new instance for the given test if one is not already
// running; otherwise it returns the existing target.
func GetOrStart(t testing.TB) string {
	target := targets[t]
	if target != "" {
		return target
	}
	target = Start(t)
	targets[t] = target
	t.Cleanup(func() {
		delete(targets, t)
	})
	return target
}

// Start starts a test-scoped spanner DB and returns the DB target.
//
// Currently requires Docker to be available in the test execution environment.
func Start(t testing.TB) string {
	const dbName = "projects/theproject/instances/theinstance/databases/thedatabase"

	var port int
	var containerName string
	{
		port = testport.FindFree(t)
		containerName = fmt.Sprintf("%s%d", containerNamePrefix, port)

		log.Debug("Starting spanner DB...")
		cmd := exec.Command(
			"docker", "run", "--detach",
			"-p", "9010:9010",
			"-p", "9020:9020",
			"--name", containerName,
			"gcr.io/cloud-spanner-emulator/emulator@sha256:636fdfc528824bae5f0ea2eca6ae307fe81092f05ec21038008bc0d6100e52fc",
		)
		cmd.Stderr = &logWriter{"docker run spanner"}
		err := cmd.Run()
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		cmd := exec.Command("docker", "kill", containerName)
		cmd.Stderr = &logWriter{"docker kill " + containerName}
		err := cmd.Run()
		require.NoError(t, err)
	})

	ctx := context.Background()
	log.Info("hi1")

	env := os.Environ()
	log.Infof("%s", env)

	_ = instance.NewInstanceAdminClient(ctx)
	adminClient, err := database.NewDatabaseAdminClient(ctx)

	require.NoError(t, err)
	defer adminClient.Close()

	dataClient, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		require.NoError(t, err)
	}
	defer dataClient.Close()

	dsn := "localhost:9010/" + dbName
	// Wait for the DB to start up.
	db, err := sql.Open("spanner", dsn)
	require.NoError(t, err)
	defer db.Close()

	for {
		if err := db.Ping(); err != nil {
			log.Infof("ping failed %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return dsn
	}
}

type logWriter struct {
	tag string
}

func (w *logWriter) Write(b []byte) (int, error) {
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		log.Infof("[%s] %s", w.tag, line)
	}
	return len(b), nil
}
