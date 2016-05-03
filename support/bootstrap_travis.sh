#!/usr/bin/env bash

curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
chmod +x kerl

OTP_RELEASE="OTP-$TRAVIS_OTP_RELEASE"

if [ "$TRAVIS_OTP_RELEASE" = "18.3" ]; then
    OTP_RELEASE="18.3.2"
fi

wget https://barrel-db.org/dl/erlang-18.3.2-nonroot.tar.bz2
mkdir -p $HOME/otp && tar -xf erlang-18.3.2-nonroot.tar.bz2 -C $HOME/otp/
mkdir -p $HOME/.kerl
source $HOME/otp/18.3.2/activate

./rebar3 update
