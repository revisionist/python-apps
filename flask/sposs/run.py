# Copyright 2023-2024 David Goddard.

# Except where otherwise noted, this software is licensed under the Apache
# License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at:
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# This file includes code based on boilerplate from Idris Rampurawala,
# originally under the MIT License. Original boilerplate code Copyright 2020
# by Idris Rampurawala. The full text of the MIT License for the original
# boilerplate code can be found in the accompanying file named
# 'LICENSE-MIT.txt' or at https://opensource.org/licenses/MIT.

import os
from app import create_app

app = create_app()


@app.route('/status', methods=['GET'])
def status():
    return 'Running!'


if __name__ == '__main__':
    host = os.getenv('FLASK_HOST', '127.0.0.1')
    port = os.getenv('FLASK_PORT', 5000)
    app.run(host=host, port=port)