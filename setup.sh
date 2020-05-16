sudo apt-get -y update

sudo apt install -y git cmake build-essential libboost-all-dev python3-pip parallel libprocps-dev

# install openjdk 1.8. This is for simulating Adaptive-TinyLFU. The steps has to be one by one
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install -y openjdk-8-jdk
java -version

cd lib/LightGBM-eloiseh/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8
sudo make install
cd ../../..


# dependency for mongo c driver
sudo apt-get install -y cmake libssl-dev libsasl2-dev

# installing mongo c
cd lib/mongo-c-driver-1.13.1/cmake-build/
cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF ..
make -j8
sudo make install
cd ../../..

# installing mongo-cxx
cd lib/mongo-cxx-driver-r3.4.0/build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ..
sudo make -j8
sudo make install
cd ../../..

# installing libbf
cd lib/libbf-dadd48e/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8
sudo make install
cd ../../..

# building webcachesim
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8
sudo make install
sudo ldconfig
cd ..

# install python framework
pip3 install -e .

mkdir /tmp/log
mkdir /tmp/log/webcachesim
mkdir /tmp/log/webcachesim/tasklog
