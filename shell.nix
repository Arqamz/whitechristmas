{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  name = "crime-analytics-dev";

  buildInputs = with pkgs; [
    # ============================================================================
    # Rust Development (Ingestion Layer)
    # ============================================================================
    rustc
    cargo
    rustfmt
    clippy
    rust-analyzer
    cargo-edit
    # rdkafka cmake-build feature dependencies
    cmake
    openssl
    openssl.dev

    # ============================================================================
    # Go Development (API/WebSocket Layer)
    # ============================================================================
    go
    golangci-lint
    gofumpt
    go-tools

    # ============================================================================
    # Scala & JVM (Batch & Stream Processing Layer)
    # ============================================================================
    openjdk25
    scala
    sbt
    maven
    scalafmt

    # ============================================================================
    # Big Data & Messaging (Data Pipeline Layer)
    # ============================================================================
    spark
    apacheKafka
    confluent-platform
    zookeeper

    # ============================================================================
    # Development & Formatting Tools (Code Quality)
    # ============================================================================
    nixfmt
    treefmt
    lefthook
    shellcheck
    yamllint
    eslint
    prettier
    typescript
    shfmt
    statix

    # ============================================================================
    # Build Essentials & Version Control (Core Tools)
    # ============================================================================
    gcc
    gnumake
    pkg-config
    curl
    wget
    git
    jq

    # ============================================================================
    # Database Clients (Data Storage Layer)
    # ============================================================================
    postgresql
    mongodb
    mongodb-tools

    # ============================================================================
    # Node.js & Bun (Frontend Layer)
    # ============================================================================
    bun
    nodejs_24

    # ============================================================================
    # Observability & Monitoring (Observability Layer)
    # ============================================================================
    prometheus
    grafana

    # ============================================================================
    # Utilities (Supporting Tools)
    # ============================================================================
    lsof
    netcat
    tmux
  ];

  shellHook = ''
    # =========================================================================
    # Environment Variables Setup
    # =========================================================================

    # Java/JVM Configuration (for Scala/Spark)
    export JAVA_HOME="${pkgs.openjdk25}"
    export SPARK_HOME="${pkgs.spark}"
    export KAFKA_HOME="${pkgs.apacheKafka}"

    # Go Configuration
    export GOPATH="$HOME/go"
    export PATH="$GOPATH/bin:$PATH"

    # Rust Configuration
    export CARGO_HOME="$HOME/.cargo"
    export RUSTFLAGS="-C target-cpu=native"
    export PATH="$CARGO_HOME/bin:$PATH"
    export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig:$PKG_CONFIG_PATH"
    export OPENSSL_DIR="${pkgs.openssl.dev}"
    export OPENSSL_LIB_DIR="${pkgs.openssl.out}/lib"

    # Scala Configuration
    export SCALA_HOME="${pkgs.scala}"
    export SCALA_OPTS="-Xms512m -Xmx2g"

    # Node/Bun Configuration
    export BUN_INSTALL="$HOME/.bun"
    export PATH="$BUN_INSTALL/bin:$PATH"
    export NODE_PATH="${pkgs.nodejs_24}/lib/node_modules"

    # Development Configuration
    export WORKSPACE_ROOT="$PWD"

    # =========================================================================
    # Helper Functions
    # =========================================================================

    # Function to safely get version info
    safe_version() {
      local cmd="$1"
      local args="$2"
      if command -v "$cmd" &> /dev/null; then
        $cmd $args 2>&1 | head -1
      else
        echo "not available"
      fi
    }

    # Function to get Spark version (special handling for multi-line output)
    get_spark_version() {
      if command -v spark-submit &> /dev/null; then
        spark-submit --version 2>&1 | grep -E "version|Scala" | head -3
      else
        echo "not available"
      fi
    }

    # Function to pretty-print environment info
    print_env_info() {
      echo ""
      echo "╔════════════════════════════════════════════════════════════════════════════╗"
      echo "║     Real-Time Crime Analytics & Intelligent Alert System Dev Environment   ║"
      echo "║                   Polyglot Stack: Scala + Go + Rust                        ║"
      echo "╚════════════════════════════════════════════════════════════════════════════╝"
      echo ""
      
      echo "  📦 Core Languages:"
      echo "    ✓ Scala:         $(safe_version ${pkgs.scala}/bin/scala '-version')"
      echo "    ✓ Go:            $(safe_version ${pkgs.go}/bin/go 'version')"
      echo "    ✓ Java:          $(safe_version ${pkgs.openjdk25}/bin/java '-version')"
      echo "    ✓ Rust:          $(safe_version rustc '--version')"
      echo ""
      
      echo "  🔄 Batch & Stream Processing (Scala + Spark):"
      echo "    ✓ Spark:"
      get_spark_version | sed 's/^/         /'
      echo "    ✓ SBT:           $(safe_version ${pkgs.sbt}/bin/sbt '--version')"
      echo "    ✓ Maven:         $(safe_version ${pkgs.maven}/bin/mvn '--version')"
      echo ""
      
      echo "  📨 Messaging & Events (Kafka):"
      echo "    ✓ Kafka:         $(safe_version ${pkgs.apacheKafka}/bin/kafka-topics.sh '--version')"
      echo "    ✓ Zookeeper:     $(if [ -f ${pkgs.zookeeper}/bin/zkServer.sh ]; then echo 'available'; else echo 'not available'; fi)"
      echo ""
      
      echo "  🗄️  Databases:"
      echo "    ✓ PostgreSQL:    $(safe_version ${pkgs.postgresql}/bin/psql '--version')"
      echo "    ✓ MongoDB:       $(safe_version ${pkgs.mongodb}/bin/mongod '--version')"
      echo ""
      
      echo "  🌐 Frontend & Node:"
      echo "    ✓ Node.js:       $(safe_version ${pkgs.nodejs_24}/bin/node '--version')"
      echo "    ✓ Bun:           $(safe_version ${pkgs.bun}/bin/bun '--version')"
      echo "    ✓ TypeScript:    $(safe_version ${pkgs.typescript}/bin/tsc '--version')"
      echo ""
      
      echo "  📊 Observability:"
      echo "    ✓ Prometheus:    $(safe_version ${pkgs.prometheus}/bin/prometheus '--version')"
      echo "    ✓ Grafana:       $(if [ -x ${pkgs.grafana}/bin/grafana-server ]; then safe_version ${pkgs.grafana}/bin/grafana-server '--version'; else echo 'available'; fi)"
      echo ""
      
      echo "  🎨 Code Formatters:"
      echo "    ✓ scalafmt:      $(safe_version scalafmt '--version')"
      echo "    ✓ gofmt:         $(if ${pkgs.go}/bin/go version &> /dev/null; then echo 'available'; else echo 'not available'; fi)"
      echo "    ✓ rustfmt:       $(safe_version rustfmt '--version')"
      echo "    ✓ Prettier:      $(safe_version prettier '--version')"
      echo "    ✓ shfmt:         $(safe_version shfmt '--version')"
      echo "    ✓ nixfmt:        $(safe_version nixfmt '--version')"
      echo "    ✓ treefmt:       $(safe_version treefmt '--version')"
      echo ""
      
      echo "  🔍 Linters & Type Checkers:"
      echo "    ✓ golangci-lint: $(safe_version golangci-lint '--version')"
      echo "    ✓ Clippy:        $(safe_version clippy-driver '--version')"
      echo "    ✓ ESLint:        $(safe_version eslint '--version')"
      echo "    ✓ statix:        $(safe_version statix '--version')"
      echo "    ✓ shellcheck:    $(safe_version shellcheck '--version')"
      echo "    ✓ yamllint:      $(safe_version yamllint '--version')"
      echo ""
      
      echo "  🛠️  Development Tools:"
      echo "    ✓ Git:           $(safe_version ${pkgs.git}/bin/git '--version')"
      echo "    ✓ Make:          $(safe_version ${pkgs.gnumake}/bin/make '--version')"
      echo "    ✓ pkg-config:    $(safe_version ${pkgs.pkg-config}/bin/pkg-config '--version')"
      echo "    ✓ lefthook:      $(safe_version lefthook '--version')"
      echo ""
      
      echo "    ╔════════════════════════════════════════════════════════════════════════════╗"
      echo "    ║                    📋 Quick Start Commands                                  ║"
      echo "    ╚════════════════════════════════════════════════════════════════════════════╝"
      echo ""
      echo "    📊 Scala & Spark (Batch & Stream Processing):"
      echo "      make hello-spark       → Run Scala + Spark hello world"
      echo "      spark-shell            → Launch interactive Scala shell with Spark"
      echo "      sbt run                → Run SBT project"
      echo "      sbt test               → Run Scala tests"
      echo ""
      echo "    🔗 Go (API & WebSocket Layer):"
      echo "      make hello-api         → Run Go API hello world"
      echo "      go mod init <module>   → Initialize a Go module"
      echo "      go run main.go         → Run a Go program"
      echo "      go test ./...          → Run Go tests"
      echo ""
      echo "    🦀 Rust (Ingestion Layer):"
      echo "      make hello-ingestion   → Run Rust ingestion hello world"
      echo "      cargo new <project>    → Create a new Rust project"
      echo "      cargo build --release  → Build optimized Rust binary"
      echo "      cargo test             → Run Rust tests"
      echo ""
      echo "    🎨 Frontend (Next.js):"
      echo "      make hello-frontend    → Initialize Next.js frontend"
      echo "      npm install            → Install npm dependencies"
      echo "      bun run dev            → Run dev server with Bun"
      echo "      npm run build          → Build for production"
      echo ""
      echo "    ✅ Quality Assurance:"
      echo "      make lint              → Lint all code (Scala, JS, Go, Rust, Nix, Shell, YAML)"
      echo "      make format            → Format all code automatically"
      echo "      make format-check      → Check formatting without changes"
      echo "      make check             → Run all checks (lint + format-check)"
      echo "      treefmt --check        → Check formatting with treefmt"
      echo "      lefthook run pre-push  → Run pre-push checks"
      echo ""
      echo "    🔧 Configuration & Setup:"
      echo "      make setup             → Setup and validate environment"
      echo "      make validate          → Comprehensive environment validation"
      echo "      nix flake update       → Update nixpkgs to latest"
      echo "      direnv allow           → Enable automatic environment loading"
      echo ""
      echo "╔════════════════════════════════════════════════════════════════════════════╗"
      echo "║                   ✨ Development Workspace Ready! ✨                       ║"
      echo "║             Polyglot stack (Scala + Go + Rust) configured                  ║"
      echo "╚════════════════════════════════════════════════════════════════════════════╝"
      echo ""
    }

    # Call on shell entry
    print_env_info
  '';
}
