# Running Migrator CLI

- Generate a new migration file
    ```shell
    cargo run -- migrate generate MIGRATION_NAME
    ```
- Apply all pending migrations
    ```shell
    cargo run
    ```
    ```shell
    cargo run -- up
    ```
- Apply first 10 pending migrations
    ```shell
    cargo run -- up -n 10
    ```
- Rollback last applied migrations
    ```shell
    cargo run -- down
    ```
- Rollback last 10 applied migrations
    ```shell
    cargo run -- down -n 10
    ```
- Drop all tables from the database, then reapply all migrations
    ```shell
    cargo run -- fresh
    ```
- Rollback all applied migrations, then reapply all migrations
    ```shell
    cargo run -- refresh
    ```
- Rollback all applied migrations
    ```shell
    cargo run -- reset
    ```
- Check the status of all migrations
    ```shell
    cargo run -- status
    ```
