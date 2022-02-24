# D4 Binding for C

## Build

To build the binding library, you can simple run the following command:

```shell
# For release build
cargo build --release --pakcage=d4binding

# For debug build
cargo build --package=d4binding
```

## Installation

You can install the D4 binding library to your system by running the script provided under this directory

```shell
./install.sh

# Or to choose the prefix to install

PREFIX=/path/to/prefix ./install.sh
```

## How to compile C program against D4 binding library

For example you have a C program that calling the D4 C API

```c
// Assuming the file name is open_d4_file.c
#include <d4.h>
int main() {
    d4_file_t* fp = d4_open("input.d4", "r");

    // Do other things at this point

    d4_close(fp);
    return 0;
}
```

Assuming you have installed the D4 binding library under `/opt/local` directory. You could be able to compile your program with the following command.

```shell
gcc open_d4_file.c -o open_d4_file -L /opt/local/lib -ld4binding -I /opt/local/include
```

If you have installed the D4 binding API to other place, you can simply replace the prefix with the actual prefix. 

## Examples of the API

There are few examples under the example directory. And those examples can be automatically build using the the `make` command.

## Reading remote D4 file

It's possible to read a D4 file live on a remote server through HTTP/HTTPS connection as well. You should be able to use the 
exactly same API for local file to open a HTTP URL.
