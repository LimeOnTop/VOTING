#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
if [ -e .env ]; then echo '.env already exists; leaving it unchanged'; exit 0; fi
umask 077
printf 'ADMIN_TOKEN=%s\nSIGNING_KEY=%s\nCOOKIE_SECURE=false\n' "$(openssl rand -hex 24)" "$(openssl rand -hex 32)" > .env
echo 'Generated .env. Use ADMIN_TOKEN from this file in /admin. Never commit it.'
