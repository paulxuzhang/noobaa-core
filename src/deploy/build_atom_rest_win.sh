#!/bin/sh
# default - clean build

CLEAN=true;
SYSTEM="demo"
ADDRESS="http://127.0.0.1:5001"
ACCESS_KEY="123"
SECRET_KEY="abc"
#extract parms
while [[ $# > 0 ]]; do
  key=$(echo $1 | sed "s:\(.*\)=.*:\1:")
  case $key in
      --system_id)
      SYSTEM_ID=$(echo $1 | sed "s:.*=\(.*\):\1:")
      ;;
      --clean)
      CLEAN=$(echo $1 | sed "s:.*=\(.*\):\1:")
      ;;
      --system)
      SYSTEM=$(echo $1 | sed "s:.*=\(.*\):\1:")
      ;;
      --address)
      ADDRESS=$(echo $1 | sed "s:.*=\(.*\):\1:")
      ;;
    --access_key)
      ACCESS_KEY=$(echo $1 | sed "s:.*=\(.*\):\1:")
      ;;
    --secret_key)
      SECRET_KEY=$(echo $1 | sed "s:.*=\(.*\):\1:")
      ;;
    *)
      usage
      # unknown option
      ;;
  esac
  shift
done

echo "SYSTEM:$SYSTEM"
echo "CLEAN BUILD:$CLEAN"
echo "ADDRESS:$ADDRESS"
echo "ACCESS_KEY:$ACCESS_KEY"
echo "SECRET_KEY:$SECRET_KEY"

if [ "$CLEAN" = true ] ; then
        echo "delete old files"
        rm -rf build/windows_s3
        mkdir build/windows_s3
        cd build/windows_s3
        mkdir ./ssl/
        echo "copy files"
        cp ../../binding.gyp .
        cp ../../images/noobaa_icon24.ico .
        cp ../../src/deploy/7za.exe .
        cp ../../src/deploy/wget.exe  .
        curl -L http://nodejs.org/dist/v0.10.33/openssl-cli.exe > openssl.exe
        cp ../../src/deploy/openssl.cnf  ./ssl/
        cp ../../src/deploy/NooBaa_Agent_wd.exe .
        cp ../../package.json .
        cp ../../config.js .
        mkdir ./src/
        cp -R ../../src/util ./src/
        cp -R ../../src/s3 ./src/
        cp -R ../../src/rpc ./src/
        cp -R ../../src/api ./src/
        echo "npm install"
        sed -i '' '/atom-shell/d' package.json
        sed -i '' '/gulp/d' package.json
        sed -i '' '/bower/d' package.json
        sed -i '' '/bootstrap/d' package.json
        sed -i '' '/browserify"/d' package.json
        pwd
        npm install -dd
        curl -L http://nodejs.org/dist/v0.10.32/node.exe > node.exe
        #echo "Downloading atom-shell for windows"
        #curl -L https://github.com/atom/atom-shell/releases/download/v0.17.1/atom-shell-v0.17.1-win32-ia32.zip > atom-shell.zip
        #unzip atom-shell.zip -d atom-shell
        #echo "create update.tar"
        #tar -cvf update_agent.tar ./atom-shell ./node_modules ./src ./config.js ./package.json ./agent_conf.json
else
    cd build/windows_s3
fi
echo "create agent conf"
echo '{' > agent_conf.json
echo '    "dbg_log_level": 2,' >> agent_conf.json
echo '    "address": "'"$ADDRESS"'",' >> agent_conf.json
echo '    "system": "'"$SYSTEM"'",' >> agent_conf.json
echo '    "tier": "nodes",' >> agent_conf.json
echo '    "prod": "true",' >> agent_conf.json
echo '    "bucket": "files",' >> agent_conf.json
echo '    "root_path": "./agent_storage/",' >> agent_conf.json
echo '    "access_key":"'"$ACCESS_KEY"'",' >> agent_conf.json
echo '    "secret_key":"'"$SECRET_KEY"'"' >> agent_conf.json
echo '}' >> agent_conf.json

cat agent_conf.json

echo "make installer"
pwd

cp ../../src/deploy/atom_rest_win.nsi ../../src/deploy/atom_rest_win.bak
sed -i '' "s/<SYSTEM_ID>/$SYSTEM_ID/g" ../../src/deploy/atom_rest_win.nsi

makensis -NOCD ../../src/deploy/atom_rest_win.nsi

mv ../../src/deploy/atom_rest_win.bak ../../src/deploy/atom_rest_win.nsi

echo "uploading to S3"

s3cmd -P put noobaa-s3rest-setup.exe s3://noobaa-core/systems/$SYSTEM_ID/noobaa-s3rest.exe
