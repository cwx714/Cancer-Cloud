# analysis-server

analysis-server整体开发架构做了重构,部署顺序：

1. 进入analysis-proto：执行mvn clean install

1. 进入rda-server：执行mvn clean package

1. 进入hta-server：执行mvn clean package
