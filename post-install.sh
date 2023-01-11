#!/bin/sh

cleanInstall() {
  printf "\033[32m Post Install of an clean install\033[0m\n"

  printf "\033[32m Reload the service unit from disk\033[0m\n"
  systemctl daemon-reload ||:
  systemctl unmask redis-go-to-master.service ||:
  systemctl enable redis-go-to-master.service ||:
  systemctl start redis-go-to-master.service ||:
}

upgrade() {
  printf "\033[32m Post Install of an upgrade\033[0m\n"

  systemctl daemon-reload ||:
  systemctl restart redis-go-to-master.service ||:
}

# check if this is a clean install or an upgrade
action="$1"
if  [ "$1" = "configure" ] && [ -z "$2" ]; then
  # Alpine linux does not pass args, and deb passes $1=configure
  action="install"
elif [ "$1" = "configure" ] && [ -n "$2" ]; then
  # deb passes $1=configure $2=<current version>
  action="upgrade"
fi

case "$action" in
  "1" | "install")
    cleanInstall
    ;;
  "2" | "upgrade")
    upgrade
    ;;
esac
