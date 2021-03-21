class DataprocCreateClusterConfig:
    def __init__(self, config):
        self.config = config
        self.dataproc_config = config["dataproc"]
        self.bucket_config = config["dataproc"]["bucket"]


    def _build_config_bucket(self, cluster_config):
        config_bucket = self.bucket_config["jobs"]
        cluster_config['config_bucket'] = config_bucket
        return cluster_config


    def _build_gce_cluster_config(self, cluster_config):
        gce_cluster_config = {
            "zone_uri": self.config["zone"],
            "metadata": self.dataproc_config["metadata"]
        }
        cluster_config["gce_cluster_config"] = gce_cluster_config
        return cluster_config


    def _build_master_config(self, cluster_config):
        master_config = {
            "num_instances": 1,
            "machine_type_uri": self.dataproc_config["master_machine_type"],
            "disk_config": {
                "boot_disk_type": self.dataproc_config["master_disk_type"],
                "boot_disk_size_gb": self.dataproc_config["master_disk_size"]
            }
        }
        cluster_config["master_config"] = master_config
        return cluster_config


    def _build_worker_config(self, cluster_config):
        worker_config = {
            "num_instances": self.dataproc_config["num_workers"],
            "machine_type_uri": self.dataproc_config["worker_machine_type"],
            "disk_config": {
                "boot_disk_type": self.dataproc_config["worker_disk_type"],
                "boot_disk_size_gb": self.dataproc_config["worker_disk_size"]
            }
        }
        cluster_config["worker_config"] = worker_config
        return cluster_config


    def _build_software_config(self, cluster_config):
        software_config = {
            "image_version": self.dataproc_config["image_version"],
            "optional_components": ["ANACONDA"], 
            "properties": {
                "hive:hive.metastore.warehouse.dir": f"gs://{self.bucket_config['data_lake']}/hive-warehouse", 
                "core:fs.defaultFS": f"gs://{self.bucket_config['data_lake']}"
            }
        }
        cluster_config["software_config"] = software_config
        return cluster_config


    def _build_initialization_actions(self, cluster_config):
        initialization_actions = [
            {
                "executable_file": f"gs://{self.bucket_config['jobs']}/initialization_actions/connectors.sh"
            }
        ]
        cluster_config['initialization_actions'] = initialization_actions
        return cluster_config


    def _build_cluster_config(self):
        cluster_config = {
            "config_bucket": None,
            "gce_cluster_config": None,
            "master_config": None,
            "worker_config": None,
            "software_config": None,
            "initialization_actions": None
        }
        
        cluster_config = self._build_config_bucket(cluster_config)
        cluster_config = self._build_gce_cluster_config(cluster_config)
        cluster_config = self._build_master_config(cluster_config)
        cluster_config = self._build_worker_config(cluster_config)
        cluster_config = self._build_software_config(cluster_config)
        # cluster_config = self._build_initialization_actions(cluster_config)

        return cluster_config


    @classmethod
    def make(cls, config):
        return cls(config)._build_cluster_config()

