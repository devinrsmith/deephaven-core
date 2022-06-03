#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAVA_HOME="${JAVA_HOME}"

./gradlew py-server:assemble py-embedded-server:assemble

python3 -m venv /tmp/py-embedded-server
/tmp/py-embedded-server/bin/pip install --upgrade pip setuptools
/tmp/py-embedded-server/bin/pip install -r docker/server/src/main/server/requirements.txt
/tmp/py-embedded-server/bin/pip install \
  py/server/build/wheel/deephaven-0.14.0-py3-none-any.whl \
  py/embedded-server/build/wheel/deephaven_server-0.14.0-py3-none-any.whl

/tmp/py-embedded-server/bin/python -i <(cat <<EOF
from deephaven_server import *
s = Server()
s.start()

from deephaven import *
ticking_table = time_table('00:00:01').update_view(formulas=["Col1 = i % 2"])
print(ticking_table)
EOF
)
