{
  inputs = { nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable"; };

  outputs = { nixpkgs, ... }:
    let
      supportSystems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportSystems;
    in
    {
      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixpkgs-fmt);

      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          duckdb-131 = pkgs.stdenv.mkDerivation rec {
            pname = "duckdb";
            version = "1.3.1";
            src = pkgs.fetchurl {
              url =
                "https://github.com/duckdb/duckdb/releases/download/v${version}/libduckdb-linux-amd64.zip";
              sha256 = "0fzg0a9gypsny8znr3s6zyrn81x6ara66zqd9p1h31hwcnq5r45c";
            };
            nativeBuildInputs = with pkgs; [ unzip ];
            unpackPhase = ''
              unzip $src
            '';
            installPhase = ''
              mkdir -p $out/lib $out/include $out/bin
              cp libduckdb.so $out/lib/
              cp duckdb.h $out/include/
            '';

            meta = with pkgs.lib; {
              description = "DuckDB v1.3.1 library";
              platforms = platforms.linux;
            };
          };

          duckdb-cli-131 = pkgs.stdenv.mkDerivation rec {
            pname = "duckdb-cli";
            version = "1.3.1";

            src = pkgs.fetchurl {
              url =
                "https://github.com/duckdb/duckdb/releases/download/v${version}/duckdb_cli-linux-amd64.zip";
              sha256 = "027w64fyp114cqf6xckx1aiza7a72d8n0rfjkl49d53rr5b02gm4";
            };

            nativeBuildInputs = with pkgs; [ unzip patchelf autoPatchelfHook ];
            buildInputs = with pkgs; [ stdenv.cc.cc.lib ];

            unpackPhase = ''
              unzip $src
            '';

            installPhase = ''
              mkdir -p $out/bin
              cp duckdb $out/bin/
              chmod +x $out/bin/duckdb
            '';

            fixupPhase = ''
              patchelf --set-interpreter ${pkgs.glibc}/lib/ld-linux-x86-64.so.2 $out/bin/duckdb
              patchelf --set-rpath ${pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.glibc ]} $out/bin/duckdb
            '';

            meta = with pkgs.lib; {
              description = "DuckDB CLI v1.3.1";
              platforms = platforms.linux;
            };
          };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              openssl.dev
              pkg-config
              sqlite.dev
              duckdb-131
              duckdb-cli-131
            ];
            shellHook = ''
              export LD_LIBRARY_PATH="${duckdb-131}/lib:$LD_LIBRARY_PATH"
              export PKG_CONFIG_PATH="${duckdb-131}/lib/pkgconfig:$PKG_CONFIG_PATH"
            '';
          };
        }
      );
    };
}
