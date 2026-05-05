{
  description = "WhiteChristmas: Real-Time Crime Analytics & Intelligent Alert System - Development Environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config = {
            allowUnfreePredicate =
              pkg:
              builtins.elem (nixpkgs.lib.getName pkg) [
                "mongodb"
              ];
          };
        };
      in
      {
        devShells.default = import ./shell.nix { inherit pkgs; };
      }
    );
}
