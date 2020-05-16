cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8
sudo make install
sudo ldconfig
cd ..