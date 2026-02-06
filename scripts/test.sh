#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Local test script for wasmedge-db-examples
#
# Usage:
#   ./scripts/test.sh              # Install tools, build all, run all tests
#   ./scripts/test.sh --build-only # Install tools and build only (no DB needed)
#   ./scripts/test.sh --skip-install # Skip tool installation
#
# Environment variables (override defaults):
#   MYSQL_URL        - MySQL connection URL (default: mysql://root:root@localhost:3306/mysql)
#   REDIS_URL        - Redis connection URL (default: redis://localhost/)
#   POSTGRES_URL     - PostgreSQL connection URL (default: postgres://wasmedge:mysecret@localhost/testdb)
#   GREPTIMEDB_URL   - GreptimeDB connection URL (default: mysql://localhost:4002/public)
#   WASI_SDK_VERSION - WASI SDK version to install (default: 25)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
BUILD_ONLY=false
SKIP_INSTALL=false
MYSQL_URL="${MYSQL_URL:-mysql://root:root@localhost:3306/mysql}"
REDIS_URL="${REDIS_URL:-redis://localhost/}"
POSTGRES_URL="${POSTGRES_URL:-postgres://wasmedge:mysecret@localhost/testdb}"
GREPTIMEDB_URL="${GREPTIMEDB_URL:-mysql://localhost:4002/public}"
WASI_SDK_VERSION="${WASI_SDK_VERSION:-25}"

# Parse arguments
for arg in "$@"; do
  case "$arg" in
    --build-only)  BUILD_ONLY=true ;;
    --skip-install) SKIP_INSTALL=true ;;
    --help|-h)
      sed -n '3,18p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[PASS]${NC} $*"; PASS=$((PASS + 1)); }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; FAIL=$((FAIL + 1)); }
skip()  { echo -e "${YELLOW}[SKIP]${NC} $*"; SKIP=$((SKIP + 1)); }
section() { echo -e "\n${BLUE}========== $* ==========${NC}\n"; }

detect_os() {
  OS="$(uname -s)"
  ARCH="$(uname -m)"
  case "$OS" in
    Linux)  PLATFORM="linux" ;;
    Darwin) PLATFORM="macos" ;;
    *)      echo "Unsupported OS: $OS"; exit 1 ;;
  esac
  info "Detected platform: $PLATFORM ($ARCH)"
}

# ============================================================================
# Tool installation
# ============================================================================

install_rust() {
  section "Installing Rust"
  if command -v rustup &>/dev/null; then
    info "rustup found, updating to latest stable..."
    rustup update stable
  else
    info "Installing Rust via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
  fi
  rustup target add wasm32-wasip1
  info "Rust version: $(rustc --version)"
  info "Cargo version: $(cargo --version)"
  info "Target wasm32-wasip1 installed"
}

install_wasmedge() {
  section "Installing WasmEdge"
  if [[ "$PLATFORM" == "macos" ]]; then
    curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash
    export PATH="$HOME/.wasmedge/bin:$PATH"
  else
    curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash
    source "$HOME/.wasmedge/env" 2>/dev/null || true
    export PATH="$HOME/.wasmedge/bin:$PATH"
  fi
  info "WasmEdge version: $(wasmedge --version | head -1)"
}

install_wasi_sdk() {
  section "Installing WASI SDK $WASI_SDK_VERSION"

  # Determine the correct archive name for this platform
  case "$PLATFORM-$ARCH" in
    macos-arm64)  WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-arm64-macos" ;;
    macos-x86_64) WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-x86_64-macos" ;;
    linux-x86_64) WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-x86_64-linux" ;;
    linux-aarch64) WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-arm64-linux" ;;
    *) echo "Unsupported platform for WASI SDK: $PLATFORM-$ARCH"; exit 1 ;;
  esac

  WASI_SDK_INSTALL_DIR="/tmp/${WASI_SDK_ARCHIVE}"

  if [[ -d "$WASI_SDK_INSTALL_DIR" && -x "$WASI_SDK_INSTALL_DIR/bin/clang" ]]; then
    info "WASI SDK already installed at $WASI_SDK_INSTALL_DIR"
  else
    info "Downloading $WASI_SDK_ARCHIVE..."
    curl -sL "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/${WASI_SDK_ARCHIVE}.tar.gz" | tar xz -C /tmp/
  fi

  export WASI_SDK_PATH="$WASI_SDK_INSTALL_DIR"
  export CC_wasm32_wasip1="$WASI_SDK_INSTALL_DIR/bin/clang"
  export AR_wasm32_wasip1="$WASI_SDK_INSTALL_DIR/bin/ar"

  info "WASI SDK path: $WASI_SDK_PATH"
  info "CC_wasm32_wasip1: $CC_wasm32_wasip1"
  info "AR_wasm32_wasip1: $AR_wasm32_wasip1"
}

install_build_deps() {
  section "Installing build dependencies"
  if [[ "$PLATFORM" == "linux" ]]; then
    if ! command -v cc &>/dev/null; then
      info "Installing build-essential..."
      sudo apt-get update -qq
      sudo apt-get install -y -qq build-essential
    fi
    if ! command -v clang &>/dev/null; then
      info "Installing clang..."
      sudo apt-get install -y -qq clang llvm
    fi
  fi
  # macOS: Xcode command line tools provide cc/clang
  info "Build dependencies ready"
}

# ============================================================================
# Build helpers
# ============================================================================

build_project() {
  local name="$1"
  local dir="$ROOT_DIR/$name"

  info "Building $name..."
  if (cd "$dir" && cargo build --target wasm32-wasip1 --release); then
    ok "Build: $name"
    return 0
  else
    fail "Build: $name"
    return 1
  fi
}

# ============================================================================
# Test helpers
# ============================================================================

run_wasm() {
  local wasm_path="$1"
  shift
  wasmedge "$@" "$wasm_path" 2>&1 || true
}

test_mysql_async() {
  section "Test: mysql_async"
  local wasm="$ROOT_DIR/mysql_async/target/wasm32-wasip1/release/crud.wasm"
  if [[ ! -f "$wasm" ]]; then
    skip "mysql_async (not built)"
    return
  fi

  info "Running mysql_async with DATABASE_URL=$MYSQL_URL"
  local resp
  resp=$(run_wasm "$wasm" --env "DATABASE_URL=$MYSQL_URL")
  echo "$resp"
  if [[ $resp == *"Bobcat"* ]]; then
    ok "Test: mysql_async"
  else
    fail "Test: mysql_async (expected 'Bobcat' in output)"
  fi
}

test_greptimedb() {
  section "Test: greptimedb"
  local wasm="$ROOT_DIR/greptimedb/target/wasm32-wasip1/release/greptimedb.wasm"
  if [[ ! -f "$wasm" ]]; then
    skip "greptimedb (not built)"
    return
  fi

  info "Running greptimedb with DATABASE_URL=$GREPTIMEDB_URL"
  local resp
  resp=$(run_wasm "$wasm" --env "DATABASE_URL=$GREPTIMEDB_URL")
  echo "$resp"
  ok "Test: greptimedb (ran)"
}

test_mysql_simple() {
  section "Test: mysql (simple)"
  local query_wasm="$ROOT_DIR/mysql/target/wasm32-wasip1/release/query.wasm"
  local insert_wasm="$ROOT_DIR/mysql/target/wasm32-wasip1/release/insert.wasm"

  if [[ ! -f "$query_wasm" ]]; then
    skip "mysql simple (not built)"
    return
  fi

  info "Running mysql query..."
  local resp
  resp=$(run_wasm "$query_wasm" --env "DATABASE_URL=$MYSQL_URL")
  echo "$resp"
  if [[ $resp == *"localhost"* ]]; then
    ok "Test: mysql query"
  else
    fail "Test: mysql query (expected 'localhost' in output)"
  fi

  info "Running mysql insert..."
  resp=$(run_wasm "$insert_wasm" --env "DATABASE_URL=$MYSQL_URL")
  echo "$resp"
  if [[ $resp == *"foo"* ]]; then
    ok "Test: mysql insert"
  else
    fail "Test: mysql insert (expected 'foo' in output)"
  fi
}

test_redis() {
  section "Test: redis"
  local wasm="$ROOT_DIR/redis/target/wasm32-wasip1/release/wasmedge-redis-client-examples.wasm"
  if [[ ! -f "$wasm" ]]; then
    skip "redis (not built)"
    return
  fi

  info "Running redis with REDIS_URL=$REDIS_URL"
  local resp
  resp=$(run_wasm "$wasm" --env "REDIS_URL=$REDIS_URL")
  echo "$resp"
  if [[ $resp == *"UTC"* ]]; then
    ok "Test: redis"
  else
    fail "Test: redis (expected 'UTC' in output)"
  fi
}

test_postgres() {
  section "Test: postgres"
  local wasm="$ROOT_DIR/postgres/target/wasm32-wasip1/release/crud.wasm"
  if [[ ! -f "$wasm" ]]; then
    skip "postgres (not built)"
    return
  fi

  info "Running postgres with DATABASE_URL=$POSTGRES_URL"
  local resp
  resp=$(run_wasm "$wasm" --env "DATABASE_URL=$POSTGRES_URL")
  echo "$resp"
  ok "Test: postgres (ran)"
}

test_qdrant() {
  section "Test: qdrant"
  local wasm="$ROOT_DIR/qdrant/target/wasm32-wasip1/release/qdrant_examples.wasm"
  if [[ ! -f "$wasm" ]]; then
    skip "qdrant (not built)"
    return
  fi

  info "Running qdrant..."
  local resp
  resp=$(run_wasm "$wasm")
  echo "$resp"
  ok "Test: qdrant (ran)"
}

# ============================================================================
# Main
# ============================================================================

main() {
  detect_os

  if [[ "$SKIP_INSTALL" == false ]]; then
    install_rust
    install_build_deps
    install_wasi_sdk
    install_wasmedge
  else
    info "Skipping tool installation (--skip-install)"
    # Still need to export WASI SDK vars if already installed
    case "$PLATFORM-$ARCH" in
      macos-arm64)   WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-arm64-macos" ;;
      macos-x86_64)  WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-x86_64-macos" ;;
      linux-x86_64)  WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-x86_64-linux" ;;
      linux-aarch64) WASI_SDK_ARCHIVE="wasi-sdk-${WASI_SDK_VERSION}.0-arm64-linux" ;;
    esac
    WASI_SDK_INSTALL_DIR="/tmp/${WASI_SDK_ARCHIVE}"
    if [[ -d "$WASI_SDK_INSTALL_DIR" ]]; then
      export WASI_SDK_PATH="$WASI_SDK_INSTALL_DIR"
      export CC_wasm32_wasip1="$WASI_SDK_INSTALL_DIR/bin/clang"
      export AR_wasm32_wasip1="$WASI_SDK_INSTALL_DIR/bin/ar"
    fi
  fi

  # Source cargo env if not already in PATH
  if ! command -v cargo &>/dev/null; then
    source "$HOME/.cargo/env" 2>/dev/null || true
  fi
  # Source wasmedge env if not already in PATH
  if ! command -v wasmedge &>/dev/null; then
    source "$HOME/.wasmedge/env" 2>/dev/null || true
    export PATH="$HOME/.wasmedge/bin:$PATH"
  fi

  section "Building all projects"
  build_project mysql || true
  build_project mysql_async || true
  build_project postgres || true
  build_project redis || true
  build_project qdrant || true
  build_project greptimedb || true
  build_project anna || true

  if [[ "$BUILD_ONLY" == true ]]; then
    info "Build-only mode, skipping tests"
  else
    section "Running integration tests"
    info "Tests require running database services."
    info "Override connection URLs via environment variables if needed."
    echo ""

    test_mysql_async
    test_greptimedb
    test_mysql_simple
    test_redis
    test_postgres
    test_qdrant
  fi

  # Summary
  section "Summary"
  echo -e "  ${GREEN}Passed: $PASS${NC}"
  echo -e "  ${RED}Failed: $FAIL${NC}"
  echo -e "  ${YELLOW}Skipped: $SKIP${NC}"
  echo ""

  if [[ $FAIL -gt 0 ]]; then
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
  else
    echo -e "${GREEN}All done.${NC}"
    exit 0
  fi
}

main "$@"
