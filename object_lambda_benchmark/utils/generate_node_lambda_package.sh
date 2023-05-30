npm install
cd node_modules
zip -r ../deployment-package.zip .
cd ..
zip -g deployment-package.zip index.js
zip -g deployment-package.zip package.json
zip -g deployment-package.zip time_server.proto
