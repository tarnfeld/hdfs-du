(function(){

    var HDFSCalculator = function(options){
        this.options = options;

        this.verifyFilesystem = function(){
            return false; // TODO?
        };

        this.calculateSize = function(path){
            var self = this;
            return new Promise(function(resolve, reject){
                self.listPath(path).then(function(files){
                    var promises = [],
                        children = [],
                        totalSize = 0;

                    for (index in files) {
                        var file = files[index];
                        if (file.type == "FILE") {
                            totalSize += file.length;
                            postMessage({
                                success: true,
                                message: "path",
                                path: path + "/" + file.pathSuffix
                            });

                            children.push({
                                value: file.length,
                                label: path + "/" + file.pathSuffix,
                                children: []
                            });
                        } else {
                            promises.push(
                                self.calculateSize(path + "/" + file.pathSuffix)
                            );
                        }
                    }

                    Promise.all(promises).then(function(files){
                        for (index in files) {
                            var file = files[index];
                            totalSize += file.value;
                            children.push(file);
                        }

                        resolve({
                            label: path,
                            value: totalSize,
                            children: children
                        })
                    }, function(){
                        reject();
                    })
                }, function(){
                    reject();
                });
            });
        };

        this.listPath = function(path){
            var self = this;
            return new Promise(function(resolve, reject){
                var url = "http://" + self.options.base_url + path + "?op=LISTSTATUS",
                    req = new XMLHttpRequest();

                req.onreadystatechange = function(){
                    if (req.readyState == 4){
                        resolve(JSON.parse(req.responseText).FileStatuses.FileStatus);
                    }
                }

                req.open("GET", url, true);
                req.send(null);
            });
        };
    };

    self.onmessage = function(e) {
        var base_url = e.data.hdfs_namenode + "/webhdfs/v1",
            calculator = new HDFSCalculator({
                base_url: base_url
            });

        calculator.calculateSize(e.data.hdfs_path.replace(/\/$/, '')).then(function(tree){
            postMessage({
                message: "tree",
                success: true,
                tree: tree
            });
        }, function(){
            postMessage({
                success: false
            });
        });
    }

}).call(this);
