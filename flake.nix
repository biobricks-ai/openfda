{
  description = "OpenFDA biobrick";

  inputs = { 
    dev-shell.url = "github:biobricks-ai/dev-shell?rev=da0ad2bf4967585f7a8091e5e899ac3a929ae9ab";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, dev-shell, nixpkgs }: 
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      pythonWithPackages = pkgs.python3.withPackages (ps: with ps; [
        pandas
        pyarrow
        fastparquet
        pytest
        pytest-cov
        requests
        tqdm
        python-dateutil
        aiohttp
        aiofiles
      ]);
    in
    {
      devShells.${system}.default = dev-shell.devShells.${system}.default.overrideAttrs (oldAttrs: {
        buildInputs = (oldAttrs.buildInputs or []) ++ [
          pkgs.parallel
          pythonWithPackages
        ];
      });
    };
}
