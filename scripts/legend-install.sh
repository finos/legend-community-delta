##############################################################
# Step by step installation of the entirue legend stack on AWS
# author: Antoine Amend
# tested: AWS EC2 REHL X.LARGE
##############################################################

sudo yum -y upgrade
sudo yum -y install unzip
sudo yum -y install wget

LEGEND_HOME=/home/ec2-user/legend
mkdir $LEGEND_HOME

########################################
# INSTALL GIT
########################################

sudo yum -y install git
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub
# copy key to github

cd $LEGEND_HOME
git clone git@github.com:aamend/legend-pure.git
git clone git@github.com:aamend/legend-engine.git
git clone git@github.com:aamend/legend-sdlc.git
git clone git@github.com:aamend/legend-studio.git
git clone git@github.com:finos-labs/legend-delta.git

########################################
# INSTALL NODE
########################################

curl -sL https://rpm.nodesource.com/setup_16.x | sudo bash -
curl -sL https://dl.yarnpkg.com/rpm/yarn.repo | sudo tee /etc/yum.repos.d/yarn.repo
sudo yum -y install yarn
npm config set legacy-peer-deps=true

########################################
# INSTALL MAVEN
########################################

sudo yum -y install maven

########################################
# UPGRADE JAVA 11
########################################

sudo yum -y install java-11-openjdk-devel
sudo update-alternatives --config java
sudo update-alternatives --config javac
sudo update-alternatives --config java_sdk_openjdk
echo export JAVA_HOME=/usr/lib/jvm/java-openjdk >> ~/.mavenrc

########################################
# COMPILE PURE
########################################

cd $LEGEND_HOME/legend-pure
git checkout deltaLake

# maven has the tendency to search for updates on maven central for snapshots
# even when forcing --no-updates, could not seem to compile the entire stack
# hence, fooling maven with a DATABRICKS version we know does not exist on maven central
PURE_VERSION_SRC=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec`
PURE_VERSION_DST=`echo $PURE_VERSION_SRC | sed 's/-SNAPSHOT/-DATABRICKS/g'`
mvn versions:set -DnewVersion=$PURE_VERSION_DST

# some constraints on pom.xml are preventing us from compiling
sed -i 's/<failOnWarning>.*<\/failOnWarning>/<failOnWarning>false<\/failOnWarning>/g' pom.xml

# compile, ignoring test errors
mvn clean install -Dmaven.test.failure.ignore=true

# save version for downstream dependencies
PURE_VERSION=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec`
echo $PURE_VERSION > $LEGEND_HOME/legend-pure/.version

########################################
# COMPILE ENGINE
########################################

# download JDBC driver
wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.19/SimbaSparkJDBC42-2.6.19.1030.zip
unzip SimbaSparkJDBC42-2.6.19.1030.zip
cp SparkJDBC42.jar $LEGEND_HOME/legend-spark.jar

# get code
cd $LEGEND_HOME/legend-engine
git checkout deltaLake
PURE_VERSION=`cat $LEGEND_HOME/legend-pure/.version | head -1`

# maven has the tendency to search for updates on maven central for snapshots
# even when forcing --no-updates, could not seem to compile the entire stack
# hence, fooling maven with a DATABRICKS version we know does not exist on maven central
ENGINE_VERSION_SRC=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec`
ENGINE_VERSION_DST=`echo $ENGINE_VERSION_SRC | sed 's/-SNAPSHOT/-DATABRICKS/g'`
mvn versions:set -DnewVersion=$ENGINE_VERSION_DST

# some constraints on pom.xml are preventing us from compiling
# ensure we're using the correct compiled PURE version
sed -i "s/<failOnWarning>.*<\/failOnWarning>/<failOnWarning>false<\/failOnWarning>/g" pom.xml
sed -i "s/<legend.pure.version>.*<\/legend.pure.version>/<legend.pure.version>$PURE_VERSION<\/legend.pure.version>/g" pom.xml

# compile
mvn clean install -Dmaven.test.failure.ignore=true

# save version for downstream dependencies
ENGINE_VERSION=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec`
echo $ENGINE_VERSION > $LEGEND_HOME/legend-engine/.version

########################################
# COMPILE SDLC
########################################

cd $LEGEND_HOME/legend-sdlc
PURE_VERSION=`cat $LEGEND_HOME/legend-pure/.version | head -1`
ENGINE_VERSION=`cat $LEGEND_HOME/legend-engine/.version | head -1`

# maven has the tendency to search for updates on maven central for snapshots
# even when forcing --no-updates, could not seem to compile the entire stack
# hence, fooling maven with a DATABRICKS version we know does not exist on maven central
SDLC_VERSION_SRC=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec`
SDLC_VERSION_DST=`echo $SDLC_VERSION_SRC | sed 's/-SNAPSHOT/-DATABRICKS/g'`
mvn versions:set -DnewVersion=$SDLC_VERSION_DST

# some constraints on pom.xml are preventing us from compiling
# ensure we use the compiled PURE and ENGINE versions
sed -i "s/<failOnWarning>.*<\/failOnWarning>/<failOnWarning>false<\/failOnWarning>/g" pom.xml
sed -i "s/<legend.pure.version>.*<\/legend.pure.version>/<legend.pure.version>$PURE_VERSION<\/legend.pure.version>/g" pom.xml
sed -i "s/<legend.engine.version>.*<\/legend.engine.version>/<legend.engine.version>$ENGINE_VERSION<\/legend.engine.version>/g" pom.xml

# compile
mvn clean install -Denforcer.skip=true -Dmaven.test.failure.ignore=true

# save version for downstream dependencies
SDLC_VERSION=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec`
echo $SDLC_VERSION > $LEGEND_HOME/legend-sdlc/.version

########################################
# COMPILE STUDIO
########################################

cd $LEGEND_HOME/legend-studio
git checkout deltaLake
yarn install
yarn setup

# Modify conf to listen to all interfaces and connect to engine / sdlc
IP=`curl http://checkip.amazonaws.com`
sed -i "s/devServerOptions,/devServerOptions, host: '0.0.0.0'/g" packages/legend-studio-deployment/webpack.config.js
sed -i "s/localhost/$IP/g" packages/legend-studio-deployment/dev/config.json

########################################
# COMPILE LEGEND DELTA
########################################

cd $LEGEND_HOME/legend-delta
PURE_VERSION=`cat $LEGEND_HOME/legend-pure/.version | head -1`
ENGINE_VERSION=`cat $LEGEND_HOME/legend-engine/.version | head -1`
SDLC_VERSION=`cat $LEGEND_HOME/legend-sdlc/.version | head -1`

# some constraints on pom.xml are preventing us from compiling
# ensure we use the compiled PURE and ENGINE versions
sed -i "s/<legend.pure.version>.*<\/legend.pure.version>/<legend.pure.version>$PURE_VERSION<\/legend.pure.version>/g" pom.xml
sed -i "s/<legend.engine.version>.*<\/legend.engine.version>/<legend.engine.version>$ENGINE_VERSION<\/legend.engine.version>/g" pom.xml
sed -i "s/<legend.sdlc.version>.*<\/legend.sdlc.version>/<legend.sdlc.version>$SDLC_VERSION<\/legend.sdlc.version>/g" pom.xml

# compile
mvn clean install -Pshaded
