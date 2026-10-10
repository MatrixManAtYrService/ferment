{
  description = "ferment";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "git+https://github.com/NixOS/nixpkgs?ref=nixos-26.05";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs = {
        pyproject-nix.follows = "pyproject-nix";
        uv2nix.follows = "uv2nix";
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = {
    self, nixpkgs, flake-utils, uv2nix, pyproject-nix, pyproject-build-systems,
  }:
    {
      # fermentGo / fermentFakeable: Go programs that obey FAKETIME
      # (see nix/fakeable-go.nix). Consumed by the VM-side flake of a
      # concrete fermenter.
      overlays.default = import ./nix/fakeable-go.nix;
    }
    // flake-utils.lib.eachDefaultSystem (system:
      let
        lib = nixpkgs.lib;
        pkgs = import nixpkgs { inherit system; };

        python = pkgs.python312;
        workspace = uv2nix.lib.workspace.loadWorkspace {
          workspaceRoot = ./.;
        };

        pyprojectOverlay = workspace.mkPyprojectOverlay {
          sourcePreference = "wheel";
        };

        pythonSet = (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope (
          lib.composeManyExtensions [
            pyproject-build-systems.overlays.default
            pyprojectOverlay
          ]
        );

        editableOverlay = workspace.mkEditablePyprojectOverlay {
          root = "$REPO_ROOT";
        };

        editableHatchling = final: prev: {
          ferment = prev.ferment.overrideAttrs (old: {
            nativeBuildInputs =
              old.nativeBuildInputs
              ++ final.resolveBuildSystem {
                editables = [ ];
              };
          });
        };

        # Editable venv: ferment installs as metadata plus a .pth pointing at
        # $REPO_ROOT, so code edits apply on the next invocation, no rebuild.
        # Includes the dev group (pytest).
        editableVenv = (pythonSet.overrideScope (
          lib.composeManyExtensions [
            editableOverlay
            editableHatchling
          ]
        )).mkVirtualEnv "ferment" workspace.deps.all;
      in
      {
        packages = {
          default = pythonSet.mkVirtualEnv "ferment" workspace.deps.default;
          editable = editableVenv;
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [
            editableVenv
            pkgs.uv
            pkgs.ruff
            pkgs.lima
          ];
          env = {
            UV_NO_SYNC = "1";
            UV_PYTHON = python.interpreter;
            UV_PYTHON_DOWNLOADS = "never";
          };
          shellHook = ''
            export REPO_ROOT=$(pwd)
            # A stray PYTHONPATH (e.g. from another nix python app's setup
            # hook) would shadow the venv's site-packages; drop it.
            unset PYTHONPATH
          '';
        };
      });
}
