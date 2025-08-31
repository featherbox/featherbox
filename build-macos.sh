#!/bin/bash
set -e

echo "🔨 Building FeatherBox for macOS"

if ! command -v pnpm &> /dev/null; then
    echo "❌ pnpm is not installed. Please install pnpm first."
    exit 1
fi

add_target_if_needed() {
    local target=$1
    if ! rustup target list --installed | grep -q "$target"; then
        echo "📦 Adding $target target..."
        rustup target add "$target"
    fi
}

add_target_if_needed "x86_64-apple-darwin"
add_target_if_needed "aarch64-apple-darwin"

echo "🏗️  Building for x86_64 (Intel Mac)..."
cargo build --release --target x86_64-apple-darwin

echo "🏗️  Building for aarch64 (Apple Silicon Mac)..."
cargo build --release --target aarch64-apple-darwin

echo "📦 Creating distribution packages..."
mkdir -p dist

cp target/x86_64-apple-darwin/release/fbox dist/fbox-macos-x86_64
tar -czvf dist/fbox-macos-x86_64.tar.gz -C dist fbox-macos-x86_64

cp target/aarch64-apple-darwin/release/fbox dist/fbox-macos-aarch64
tar -czvf dist/fbox-macos-aarch64.tar.gz -C dist fbox-macos-aarch64

echo "✅ macOS builds completed!"
echo "📁 x86_64 Binary: dist/fbox-macos-x86_64"
echo "📦 x86_64 Package: dist/fbox-macos-x86_64.tar.gz"
echo "📁 ARM64 Binary: dist/fbox-macos-aarch64" 
echo "📦 ARM64 Package: dist/fbox-macos-aarch64.tar.gz"

echo ""
echo "🔍 Checking dependencies (x86_64)..."
if command -v otool &> /dev/null; then
    otool -L dist/fbox-macos-x86_64 || echo "⚠️  Could not check dependencies"
else
    echo "⚠️  otool not available, cannot check dependencies"
fi