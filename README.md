# DejaVu II

For now just a stub.

## Build Instructions

> You need a decent C++ compiler and `cmake`. These should be very easy to obtain for linux. Outside of linux, you are on your own, but should still be easily possible.

Clone the repo and then from its directory, run the following:

    bash setup.sh

This initializes the cmake generated build scripts in `build` directory. To build the project afterwards, you can either go to `build` directory and run `make`, or from the default directory run `cmake` as follows:

    cmake --build ./build

You only need to regenerate the build scripts if you add or remove `cpp` files to the project. To do so, in the build directory run the following:

    cmake ..

## Data

The actual data is too big to be hosted on github. Contact the owners of the repo if you want them. 
