#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

bin_cargo_toml="cmd/vector/Cargo.toml"
main_cargo_toml="Cargo.toml"

header=$(sed -e '
/^\[features\]$/,$ d;
' < $bin_cargo_toml
)

features=$(sed -e '
1,/^\[features\]$/ d;
/^\[/,$ d;
/=/ !d;
s/\(\S*\) =.*$/\1 = \["vector\/\1"\]/;
' < $main_cargo_toml
)

footer=$(sed -n -e '
1,/^\[features\]$/ d;
/^\[/,$ p;
' < $bin_cargo_toml
)

echo "$header" > $bin_cargo_toml
echo -e "
[features]
# Features are synchronized with the main Cargo.toml
# To update it, please run scripts/sync-bin-features.sh" >> $bin_cargo_toml
echo "$features" >> $bin_cargo_toml
echo -e "\n$footer" >> $bin_cargo_toml
