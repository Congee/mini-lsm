{
  description = "LSM";

  inputs.nixpkgs.url      = "github:nixos/nixpkgs/nixos-unstable";
  inputs.flake-utils.url  = "github:numtide/flake-utils";
  inputs.rust-overlay.url = "github:oxalica/rust-overlay";

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
    let
      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs { inherit system; };
      codelldb = pkgs.vscode-extensions.vadimcn.vscode-lldb;
      rust = (import nixpkgs { inherit system overlays; }).rust-bin;

      # https://nixos.wiki/wiki/Nix_Cookbook#Wrapping_packages
      debugger = pkgs.runCommand "codelldb" {} ''
        mkdir -p $out/bin
        ln -s ${codelldb}/share/vscode/extensions/vadimcn.vscode-lldb/adapter/codelldb $out/bin
      '';
    in {
      devShell = pkgs.mkShell {
        nativeBuildInputs = with pkgs; [  # build time
          rust.nightly.latest.default
          rust-analyzer
          pkgconfig
          buf-language-server
          protobuf
          mold
        ];
        buildInputs = with pkgs; [  # run time
          debugger
          openssl.dev
        ]
        ++ lib.optional stdenv.isDarwin darwin.apple_sdk.frameworks.Security
        ++ lib.optional stdenv.isDarwin darwin.apple_sdk.frameworks.AppKit
        ;
        RUST_SRC_PATH = "${rust.nightly.latest.rust-src}/lib/rustlib/src/rust/library";
        # mold does not seem to use pkg-config with openssl.dev
        # LD_LIBRARY_PATH = "${pkgs.openssl.out}/lib";  XXX: causes glibc version mismatch
        # RUST_SRC_PATH = "${pkgs.rust.packages.nightly.rustPlatform.rustLibSrc}";
      };
    });
}
