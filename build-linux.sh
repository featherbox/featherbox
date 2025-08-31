#!/bin/bash
set -e

echo "🔨 Building FeatherBox for Linux (x86_64-unknown-linux-musl)"

if ! command -v pnpm &> /dev/null; then
    echo "❌ pnpm is not installed. Please install pnpm first."
    exit 1
fi

if ! rustup target list --installed | grep -q "x86_64-unknown-linux-musl"; then
    echo "📦 Adding x86_64-unknown-linux-musl target..."
    rustup target add x86_64-unknown-linux-musl
fi

echo "🏗️  Building binary..."
cargo build --release --target x86_64-unknown-linux-musl

echo "📦 Creating distribution package..."
mkdir -p dist
cp target/x86_64-unknown-linux-musl/release/fbox dist/fbox-linux-x86_64
tar -czvf dist/fbox-linux-x86_64.tar.gz -C dist fbox-linux-x86_64

echo "✅ Linux build completed!"
echo "📁 Binary: dist/fbox-linux-x86_64"
echo "📦 Package: dist/fbox-linux-x86_64.tar.gz"

echo ""
echo "🔍 Checking dependencies..."
if command -v ldd &> /dev/null; then
    ldd dist/fbox-linux-x86_64 || echo "✅ Statically linked (no dynamic dependencies)"
else
    echo "⚠️  ldd not available, cannot check dependencies"
fi