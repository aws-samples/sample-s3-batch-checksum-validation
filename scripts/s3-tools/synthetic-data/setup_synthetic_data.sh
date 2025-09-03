#!/bin/bash

# Wrapper script for synthetic dataset generation
# Provides a simple shell interface to the Python dataset generator

set -e  # Exit on any error

# Default values
BUCKET_NAME=""
PREFIX="synthetic-data/"
MAX_SIZE_GB=100
FORCE=false
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Generate synthetic test data for S3 operations testing.

BUCKET CONFIGURATION:
    Uses S3_STD_MV_TEST_BUCKET environment variable or auto-generates:
    \${S3_STD_MV_TEST_BUCKET:-"\$ACCOUNT_ID-\$REGION-s3-std-mv-test"}

OPTIONS:
    --max-size SIZE     Maximum file size in GB (default: 100)
                       Valid values: 1, 5, 10, 50, 100, 500, 1000, 5000
    --prefix PREFIX     S3 key prefix (default: synthetic-data/)
    --force             Overwrite existing files
    --verbose           Show detailed output
    --help, -h          Show this help message

ENVIRONMENT VARIABLES:
    S3_STD_MV_TEST_BUCKET    Test bucket name (required or auto-generated)

AWS CONFIGURATION:
    Uses standard AWS CLI configuration (~/.aws/config, ~/.aws/credentials)
    Set profile: export AWS_PROFILE=my-profile
    Set region: export AWS_DEFAULT_REGION=us-west-2

EXAMPLES:
    # Use environment variable (recommended)
    export S3_STD_MV_TEST_BUCKET=my-test-bucket
    $0

    # Use auto-generated bucket name
    $0 --max-size 10

    # Generate with custom prefix
    $0 --prefix "test-data/" --max-size 50

    # Force regeneration of all files
    $0 --force --max-size 50

    # Generate large dataset (1TB) - requires significant storage
    $0 --max-size 1000

GENERATED FILES:
    Based on --max-size, creates files: 1GB, 5GB, 10GB, 50GB, 100GB, 500GB, 1TB, 5TB
    
    File sizes and S3 multipart characteristics:
    - 1GB:   Simple upload or small multipart
    - 5GB:   5 × 1GB parts (5 parts)
    - 10GB:  2 × 5GB parts (2 parts)
    - 50GB:  10 × 5GB parts (10 parts)
    - 100GB: 20 × 5GB parts (20 parts)
    - 500GB: 100 × 5GB parts (100 parts)
    - 1TB:   200 × 5GB parts (200 parts)
    - 5TB:   1000 × 5GB parts (1000 parts)

LIFECYCLE MANAGEMENT:
    Automatically sets up S3 lifecycle policy to expire objects after 6 months (180 days)
    for the specified prefix to prevent accumulating storage costs.

COST ESTIMATES (S3 Standard, us-west-2):
    - 100GB dataset: ~\$2.30/month storage
    - 1TB dataset:   ~\$23/month storage
    - 5TB dataset:   ~\$115/month storage

EOF
}

# Parse command line arguments
parse_args() {
    # Check for help first
    for arg in "$@"; do
        if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
            usage
            exit 0
        fi
    done
    
    # Use environment variable or generate default bucket name
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")
    REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
    BUCKET_NAME=${S3_STD_MV_TEST_BUCKET:-"$ACCOUNT_ID-$REGION-s3-std-mv-test"}

    while [[ $# -gt 0 ]]; do
        case $1 in
            --max-size)
                MAX_SIZE_GB="$2"
                shift 2
                ;;
            --prefix)
                PREFIX="$2"
                shift 2
                ;;
            --force)
                FORCE=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# Validate arguments
validate_args() {
    # Validate bucket name
    if [[ -z "$BUCKET_NAME" ]]; then
        print_error "Bucket name cannot be empty"
        exit 1
    fi
    
    # Check if bucket name contains "unknown" (failed to get account ID)
    if [[ "$BUCKET_NAME" == *"unknown"* ]]; then
        print_error "Could not determine AWS account ID automatically"
        print_error "Please set the S3_STD_MV_TEST_BUCKET environment variable:"
        print_error "  export S3_STD_MV_TEST_BUCKET=your-test-bucket-name"
        print_error ""
        print_error "Or ensure AWS CLI is configured with valid credentials"
        exit 1
    fi

    # Validate max size
    case $MAX_SIZE_GB in
        1|5|10|50|100|500|1000|5000)
            ;;
        *)
            print_error "Invalid max-size: $MAX_SIZE_GB"
            print_error "Valid values: 1, 5, 10, 50, 100, 500, 1000, 5000"
            exit 1
            ;;
    esac

    # Warn about large sizes (but don't prompt)
    if [ "$MAX_SIZE_GB" -ge 1000 ]; then
        print_warning "Large dataset requested (${MAX_SIZE_GB}GB max)"
        print_warning "This will use significant storage and time"
        
        if [ "$MAX_SIZE_GB" -eq 5000 ]; then
            print_warning "5TB dataset costs ~\$115/month in storage"
        elif [ "$MAX_SIZE_GB" -eq 1000 ]; then
            print_warning "1TB dataset costs ~\$23/month in storage"
        fi
        echo
    fi
}

# Check prerequisites
check_prerequisites() {
    # Check if Python script exists
    local script_path="$(dirname "$0")/generate_synthetic_dataset.py"
    if [ ! -f "$script_path" ]; then
        print_error "generate_synthetic_dataset.py not found in $(dirname "$0")"
        exit 1
    fi

    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "python3 is required but not installed"
        exit 1
    fi

    # Check required Python modules
    if ! python3 -c "import boto3" 2>/dev/null; then
        print_error "boto3 Python module is required"
        print_info "Install with: pip install boto3"
        exit 1
    fi

    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is required but not installed"
        exit 1
    fi
}

# Set up lifecycle policy for the bucket prefix
setup_lifecycle_policy() {
    print_info "Setting up lifecycle policy to expire objects after 6 months..."
    
    # Create lifecycle policy JSON
    local lifecycle_policy=$(cat << EOF
{
    "Rules": [
        {
            "ID": "ExpireSyntheticTestData",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "$PREFIX"
            },
            "Expiration": {
                "Days": 180
            }
        }
    ]
}
EOF
)
    
    # Check if bucket exists and is accessible
    if ! aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
        print_warning "Bucket $BUCKET_NAME does not exist or is not accessible"
        print_info "Lifecycle policy will be skipped - please set it manually if needed"
        return 0
    fi
    
    # Apply lifecycle policy
    if echo "$lifecycle_policy" | aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET_NAME" --lifecycle-configuration file:///dev/stdin; then
        print_success "Lifecycle policy applied successfully"
        print_info "Objects with prefix '$PREFIX' will expire after 180 days (6 months)"
    else
        print_warning "Failed to apply lifecycle policy"
        print_info "You may need additional permissions or the bucket may not support lifecycle policies"
        print_info "Manual setup: AWS Console > S3 > $BUCKET_NAME > Management > Lifecycle rules"
    fi
}

# Build Python command
build_command() {
    local script_path="$(dirname "$0")/generate_synthetic_dataset.py"
    local cmd="python3 \"$script_path\" \"$BUCKET_NAME\""
    
    cmd="$cmd --prefix \"$PREFIX\""
    
    if [ "$FORCE" = true ]; then
        cmd="$cmd --force"
    fi
    
    echo "$cmd"
}

# Show configuration
show_config() {
    print_info "Synthetic Dataset Generation Configuration"
    echo "  Bucket: $BUCKET_NAME"
    echo "  Environment: S3_STD_MV_TEST_BUCKET=${S3_STD_MV_TEST_BUCKET:-"(not set)"}"
    echo "  Prefix: $PREFIX"
    echo "  Max Size: ${MAX_SIZE_GB}GB"
    echo "  Force Overwrite: $FORCE"
    echo
    
    # Show what files will be generated
    print_info "Files to be generated (up to ${MAX_SIZE_GB}GB):"
    
    local sizes=(1 5 10 50 100 500 1000 5000)
    for size in "${sizes[@]}"; do
        if [ "$size" -le "$MAX_SIZE_GB" ]; then
            if [ "$size" -eq 1000 ]; then
                echo "  - ${size}GB (1TB)"
            elif [ "$size" -eq 5000 ]; then
                echo "  - ${size}GB (5TB)"
            else
                echo "  - ${size}GB"
            fi
        fi
    done
    echo
}

# Main execution
main() {
    print_info "S3 Synthetic Dataset Generator"
    echo
    
    # Parse and validate arguments
    parse_args "$@"
    validate_args
    
    # Check prerequisites
    check_prerequisites
    
    # Show configuration
    show_config
    
    # Set up lifecycle policy
    setup_lifecycle_policy
    echo
    
    # Build and execute command
    local cmd=$(build_command)
    
    print_info "Starting dataset generation..."
    if [ "$VERBOSE" = true ]; then
        print_info "Command: $cmd"
    fi
    
    # Execute the Python script
    if eval "$cmd"; then
        print_success "Dataset generation completed successfully!"
        echo
        print_info "Generated files are available at: s3://$BUCKET_NAME/$PREFIX"
        print_info "You can now run performance tests with these files"
        echo
        print_info "Next steps:"
        echo "  # Validate the dataset"
        echo "  python3 validate_dataset.py \"$BUCKET_NAME\" --prefix \"$PREFIX\""
        echo
        echo "  # Run performance tests"
        echo "  python3 test_s3_std_mv.py \"$BUCKET_NAME\" --max-size $MAX_SIZE_GB --skip-setup"
    else
        print_error "Dataset generation failed!"
        exit 1
    fi
}

# Run main function with all arguments
main "$@"
