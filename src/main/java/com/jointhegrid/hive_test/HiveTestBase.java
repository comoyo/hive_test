/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.jointhegrid.hive_test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.HadoopTestCase;

public abstract class HiveTestBase extends HadoopTestCase {

  protected static final Path ROOT_DIR = new Path("testing");

  public HiveTestBase() throws IOException {
      super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
      try {
          Thread.sleep(1000);
      } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      Map<String, String> env = new HashMap<String, String>();
      env.putAll(System.getenv());
      if (System.getenv("HADOOP_HOME ") == null) {
          File hadoopInstall = findHadoopInstall();
          if (hadoopInstall != null ){
              env.put("HADOOP_HOME", hadoopInstall.getAbsolutePath());
              EnvironmentHack.setEnv(env);
          } else {
              throw new IOException("No HADOOP_HOME or local hadoop install in target found");
          }
      }
  }
    protected Path getDir(Path dir) {
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp").replace(' ', '+');
      dir = new Path(localPathRoot, dir);
    }
    return dir;
  }

  public void setUp() throws Exception {
    super.setUp();

    String jarFile = org.apache.hadoop.hive.ql.exec.MapRedTask.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    System.setProperty(HiveConf.ConfVars.HIVEJAR.toString(), jarFile);

    Path rootDir = getDir(ROOT_DIR);
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    Path metastorePath = new Path("/tmp/metastore_db");
      fs.delete(metastorePath, true);
      Path warehouse = new Path("/tmp/warehouse");
      fs.delete(warehouse, true);
      fs.mkdirs(warehouse);
  }
  private File findHadoopInstall() {
      File target = new File("target");
      if (!target.exists()) {
          return null;
      }
      File[] content = target.listFiles();
      boolean found = false; // unused
      File hadoopPath = null; //unused
      for (int i = 0; i < content.length; i++) {
          File currentFile = content[i];
          if (currentFile.isDirectory() && currentFile.getName().startsWith("hadoop-")) {
              return currentFile;
          }
      }
      return null;
  }

}
