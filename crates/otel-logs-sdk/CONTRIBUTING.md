This guide will help you contribute to the project. It includes guidelines to run the project locally, how to test your changes integrating with function executors, and running more advanced tests.

We use [Just](https://just.systems/man/en/) to run commands locally. Once installed from the previous link, you can see all the commands available by running `just` at the root of this repository.

Once you have Just installed, you can run commands by typing `just <command>` at the root of this repository.
The command `just install-tools` will install additional tools required for development.

## Available Just Commands

## General

- `just install-tools` - Install additional tools required for development

### Formatting

- `just fmt` - Reformat all components (Rust, Indexify, and Tensorlake)

### Linting & Code Quality

- `just check` - Check formatting of all components without modifying them
- `just lint` - Run Clippy on all Rust packages, treating warnings as errors
- `just lint-fix` - Automatically fix Clippy warnings (allows dirty working directory)

### Building

- `just build` - Build all components (Rust, Indexify, and Tensorlake)

### Testing

- `just test` - Run all tests (Rust, Indexify, and Tensorlake)
