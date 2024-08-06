package loader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/processor"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"
)

type ClickHouse struct {
	connString string
	db         *sql.DB
	logger     zerolog.Logger
}

func NewClickHouseLoader(connString string, logger zerolog.Logger) *ClickHouse {
	return &ClickHouse{connString: connString, logger: logger}
}

func (l *ClickHouse) Connect(ctx context.Context) error {
	db, err := sql.Open("clickhouse", l.connString)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Verify the connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	l.db = db
	l.logger.Debug().Msg("Successfully connected to ClickHouse")
	return nil
}

func (l *ClickHouse) Load(ctx context.Context, batch processor.RecordBatcher) error {
	if l.db == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	records := batch.Records()
	numRecords := len(records)
	if numRecords == 0 {
		l.logger.Info().Msg("No records to load")
		return nil
	}

	// Construct the value strings for each record
	var valueStrings []string
	for _, record := range records {
		values := record.DBRowEntry()
		var valueParts []string
		for _, value := range values {
			switch v := value.(type) {
			case string:
				valueParts = append(valueParts, fmt.Sprintf("'%s'", v))
			default:
				valueParts = append(valueParts, fmt.Sprintf("%v", v))
			}
		}
		valueStrings = append(valueStrings, "("+strings.Join(valueParts, ", ")+")")
	}

	// Create the insert statement
	insertStatement := "INSERT INTO " + batch.TableName() + " (" + strings.Join(batch.Columns(), ", ") + ") VALUES "
	insertStatement += strings.Join(valueStrings, ", ")

	l.logger.Debug().Msgf("Insert statement: %s", insertStatement)

	// Execute the insert statement
	_, err := l.db.ExecContext(ctx, insertStatement)
	if err != nil {
		return fmt.Errorf("failed to execute insert statement: %w", err)
	}
	return nil
}

func (l *ClickHouse) Close(ctx context.Context) error {
	if l.db != nil {
		err := l.db.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
