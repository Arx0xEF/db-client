#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>

#define BUF_SIZE 8192
//#define COMMA ','

FILE *fopen_csv(const char *filename, const char *mode);
void parse_csv(char *row, const char *delimiter, char *field[10]);

int main(int argc, char *argv[]) {

    // open csv file
    FILE *f_csv = fopen_csv("404k-Telenor.csv", "r");

    // get env variables of database for the conninfo
    char *db_name = getenv("DB_NAME");
    char *db_user = getenv("DB_USER");
    char *db_pass = getenv("DB_PASS");
    char *db_host = getenv("DB_HOST");

    // pointer to char for the environment variables
    char conninfo[256];

    // copy the env variables to the conninfo
    sprintf(conninfo, "host=%s dbname=%s user=%s password=%s", db_host, db_name, db_user, db_pass);
    //    fprintf(stdout, "%s", conninfo);

    // connect to postgresql db
    PGconn *pgconn = PQconnectdb(conninfo);

    if(PQstatus(pgconn) == CONNECTION_BAD) {
        fprintf(stderr, "Error while connecting to the database server: %s\n", PQerrorMessage(pgconn));
        PQfinish(pgconn);
        exit(1);
    }

    // print server connection status
    fprintf(stdout, "Connection Established...\n");
    fprintf(stdout, "database: %s\n", PQdb(pgconn));
    fprintf(stdout, "user: %s\n", PQuser(pgconn));
    fprintf(stdout, "host: %s\n", PQhost(pgconn));
    fprintf(stdout, "port: %s\n", PQport(pgconn));

    // create table command
    char *create_table = "CREATE TABLE IF NOT EXISTS Telenor_404k ( "
                            "id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                            "number TEXT NOT NULL, "
                            "Carrier TEXT, "
                            "name TEXT, "
                            "Gender TEXT, "
                            "Address TEXT, "
                            "JobTitle TEXT, "
                            "CompanyName TEXT, "
                            "Email TEXT, "
                            "Facebook TEXT, "
                            "Twitter TEXT );";

    // Pointer to the connection result returned by PQexec, PQexecParams
    PGresult *PGResult = PQexec(pgconn, create_table);    // execute the CREATE TABLE command
                                                          //
    if(PQresultStatus(PGResult) == PGRES_FATAL_ERROR) {   // check for FATAL ERROR during PQexec
        fprintf(stderr, "CREATE TABLE command failed: %s\n", PQerrorMessage(pgconn));
        PQclear(PGResult);                                   // clear the storage which belongs to PGresult
        PQfinish(pgconn);   // can't proceed without table
        exit(1);
    }

    /* read and parse csv file */
    //  no.of rows
    size_t row = 0;
    //  a stack buffer for csv streaming
    char csv_buf[BUF_SIZE];

    while((fgets(csv_buf, BUF_SIZE, f_csv)) != NULL) {
        // field array
        char *field[10];

        // parse csv fields into the field arrays
        for(size_t i = 0; csv_buf != '\0'; i++) {
            if(csv_buf[i] == '\r' || csv_buf[i] == '\n') {    // remove window's CRLF or linux's LF at the end of string
                csv_buf[i] = '\0';
                break;
            }
        }
        parse_csv(csv_buf, ",", field);
        /*
        // Debug  line 25441
        if(row == 6188) {
            putchar('\n');
            for(size_t i = 0; i < 10; i++) {
                fprintf(stderr, "field[%zu] = %s\n", i+1, field[i]);
            }
        }
        */

        // insert fields into the db
        PGResult = PQexecParams(pgconn, // server connection
                                "INSERT INTO Telenor_404k "
                                "( number, "
                                "Carrier, "
                                "name, "
                                "Gender, "
                                "Address, "
                                "JobTitle, "
                                "CompanyName, "
                                "Email, "
                                "Facebook, "
                                "Twitter ) "
                                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);",
                                10,
                                NULL, // the server infers the datatype for the symbols
                                (const char * const *)field,
                                NULL, // length is ignored for null and text parameters
                                NULL, // 0 for text 1 for binary and null is presumed for text strings
                                0 // 0 to obtain results in text format
                                );

        if(PQresultStatus(PGResult) != PGRES_COMMAND_OK)
            fprintf(stderr, "Error: %s\n", PQerrorMessage(pgconn));

        PQclear(PGResult);
        row++;
    }

/*
    // copy command exec
    char *copy = "COPY sistema_shyam_249k "
        "(number, "
        "Carrier, "
        "name, "
        "Gender, "
        "Address, "
        "JobTitle, "
        "CompanyName, "
        "Email, "
        "Facebook, "
        "Twitter) "
    "FROM STDIN DELIMITER ',' CSV HEADER;";
    PGResult = PQexec(pgconn, copy);
    if(PQresultStatus(PGResult) == PGRES_COPY_IN) {
        PQclear(PGResult);
        fprintf(stdout, "COPY command started: %s\n", PQerrorMessage(pgconn));
        while(fgets(csv_buf, BUF_SIZE, f_csv) != NULL) {
            if(PQputCopyData(pgconn, csv_buf, strlen(csv_buf)) == -1) {
                fprintf(stderr, "PQputCopyData failed: %s\n", PQerrorMessage(pgconn));
            }
        }
        PQputCopyEnd(pgconn, NULL);
        PGResult = PQgetResult(pgconn);
        if(PQresultStatus(PGResult) != PGRES_COMMAND_OK) {
            fprintf(stderr, "COPY failed: %s\n", PQerrorMessage(pgconn));
        }

    }
    else {
        fprintf(stderr, "COPY command failed: %s\n", PQerrorMessage(pgconn));
    }
    PQclear(PGResult);
*/

    fprintf(stdout, "%zu rows inserted into the database ", row);
    // printing last parsed row to for the debug
    fprintf(stdout, "\n\n\ncsv_buf = %s", csv_buf);

    // close the connection to the db server
  //  PQfinish(pgconn);

    /*  //  connection close confirmation to test the behaviour
    if(PQstatus(pgconn) == CONNECTION_BAD)
        fprintf(stdout, "Connection to db server has been terminated.\n");
    */
    // close the csv file
    fclose(f_csv);

    return 0;
}

FILE *fopen_csv(const char *filename, const char *mode) {
    // open csv file
    FILE *f_csv;
    if((f_csv = fopen(filename, mode)) == NULL) {
        perror("fopen");
        exit(1);
    }
    return f_csv;
}

void parse_csv(char *row, const char *delimiter, char *field[10]) {
    int field_idx = 0;
    field[field_idx] = row;
    for(size_t i = 0; row[i] != '\0'; i++) {
        if(row[i] == *delimiter) {
            row[i] = '\0';
            field[++field_idx] = &row[i+1];
        }
    }
}

