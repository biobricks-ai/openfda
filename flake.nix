{
  description = "OpenFDA biobrick";

  inputs = { 
    dev-shell.url = "github:biobricks-ai/dev-shell";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, dev-shell, nixpkgs }: 
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
    in
    {
      devShells.${system}.default = dev-shell.devShells.${system}.default.overrideAttrs (oldAttrs: {
        buildInputs = (oldAttrs.buildInputs or []) ++ [
          pkgs.parallel
        ];
      });
    };
}
