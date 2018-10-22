#!/usr/bin/env bash
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_ffa18f95aa82_key -iv $encrypted_ffa18f95aa82_iv \
    -in util/signingkey.asc.enc -out util/signingkey.asc -d
    gpg --fast-import util/signingkey.asc
fi
