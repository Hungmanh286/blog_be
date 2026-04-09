#!/bin/bash
# =============================================================================
# docker-build.sh
# Build/rebuild các image cho blog_be theo docker-compose.yaml:
#   - data_blog:latest  (build từ Dockerfile — service: app)
#   - postgres:13.2     (pull từ registry   — service: db)
#   - minio/minio:latest(pull từ registry   — service: minio)
#
# Usage:
#   ./scripts/docker-build.sh [OPTIONS]
#
# Options:
#   -h, --help        Hiển thị trợ giúp
#   -c, --clean       Xoá tất cả image của project
#   --no-cache        Build app image không dùng layer cache
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Config — khớp với docker-compose.yaml
# ---------------------------------------------------------------------------
APP_IMAGE="data_blog:latest"          # service: app  (build từ Dockerfile)
POSTGRES_IMAGE="postgres:17-alpine"        # service: db   (pull)
MINIO_IMAGE="minio/minio:latest"      # service: minio (pull)

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }

# ---------------------------------------------------------------------------
show_help() {
    cat << EOF
Usage: $0 [OPTIONS]

Build/quản lý Docker images cho blog_be.

OPTIONS:
    -h, --help      Hiển thị trợ giúp này
    -c, --clean     Xoá các image đã build/pull của project
    --no-cache      Build app image không dùng Docker cache

IMAGES:
    Tên image          Nguồn          Service
    ─────────────────────────────────────────
    ${APP_IMAGE}    Build (.)      app
    ${POSTGRES_IMAGE}          Pull registry  db
    ${MINIO_IMAGE}      Pull registry  minio
EOF
}

# ---------------------------------------------------------------------------
build_app() {
    local no_cache="${1:-}"
    log_info "Building app image: ${APP_IMAGE} ..."
    docker build \
        ${no_cache} \
        -t "${APP_IMAGE}" \
        -f "${PROJECT_ROOT}/Dockerfile" \
        "${PROJECT_ROOT}" || {
        log_error "Build thất bại!"
        exit 1
    }
    log_success "App image built: ${APP_IMAGE}"
}

pull_dependencies() {
    local images=("${POSTGRES_IMAGE}" "${MINIO_IMAGE}")
    log_info "Pulling dependency images..."
    for img in "${images[@]}"; do
        log_info "  Pulling ${img} ..."
        docker pull "${img}" || {
            log_error "Không thể pull image: ${img}"
            exit 1
        }
        log_success "  Pulled: ${img}"
    done
}

# ---------------------------------------------------------------------------
clean_images() {
    log_info "Xoá các image của project..."

    local images=("${APP_IMAGE}" "${POSTGRES_IMAGE}" "${MINIO_IMAGE}")
    for img in "${images[@]}"; do
        ids=$(docker images -q "${img}" 2>/dev/null || true)
        if [[ -n "$ids" ]]; then
            docker rmi -f $ids && log_success "Đã xoá: ${img}" \
                || log_warning "Không thể xoá: ${img}"
        else
            log_warning "Không tìm thấy image: ${img}"
        fi
    done
}

# ---------------------------------------------------------------------------
show_summary() {
    echo ""
    log_success "=== BUILD HOÀN TẤT ==="
    echo ""
    echo "  Images sẵn sàng:"
    docker images --format "  {{.Repository}}:{{.Tag}}  ({{.Size}})" \
        | grep -E "^  (data_blog|postgres|minio)" || true
    echo ""
}

# ---------------------------------------------------------------------------
main() {
    local no_cache=""

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--clean)
                clean_images
                exit 0
                ;;
            --no-cache)
                no_cache="--no-cache"
                ;;
            *)
                log_error "Option không hợp lệ: $1"
                show_help
                exit 1
                ;;
        esac
        shift
    done

    echo "========================================"
    echo "     BLOG BACKEND — Docker Build        "
    echo "========================================"
    echo ""

    cd "${PROJECT_ROOT}"

    # 1. Build app image từ Dockerfile
    build_app "${no_cache}"
    echo ""

    # 2. Pull các image dependency (db, minio)
    pull_dependencies
    echo ""

    show_summary
}

trap 'echo ""; log_warning "Script bị ngắt. Thoát..."' INT TERM

main "$@"
