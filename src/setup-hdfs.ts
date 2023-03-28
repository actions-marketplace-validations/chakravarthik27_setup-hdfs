import * as core from '@actions/core';
import { downloadTool, extractTar, cacheDir } from '@actions/tool-cache';
import { exec } from 'child_process';
import * as fs from 'fs';
import { promisify } from 'util';

const writeFile = promisify(fs.writeFile);

async function setup() {
  // Fetch user input.
  const hdfsVersion = core.getInput('hdfs-version');

  const hdfsUrl = `https://archive.apache.org/dist/hadoop/common/hadoop-${hdfsVersion}/hadoop-${hdfsVersion}.tar.gz`;

  // Download hdfs and extract.
  const hdfsTar = await downloadTool(hdfsUrl);
  const hdfsFolder = (await extractTar(hdfsTar)) + `/hadoop-${hdfsVersion}`;

  const coreSite = `<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>runner</value>
    </property>
</configuration>`;
  await writeFile(`${hdfsFolder}/etc/hadoop/core-site.xml`, coreSite);

  const hdfsSite = `<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>localhost:9870</value>
    </property>
    <property>
        <name>dfs.secondary.http.address</name>
        <value>localhost:9100</value>
    </property>
</configuration>`;
  await writeFile(`${hdfsFolder}/etc/hadoop/hdfs-site.xml`, hdfsSite);

  const hdfsHome = await cacheDir(hdfsFolder, 'hdfs', hdfsVersion);

  // Setup self ssh connection.
  // Fix permission issues: https://github.community/t/ssh-test-using-github-action/166717/12
  let cmd;
  if (process.platform === 'win32') {
    cmd = `icacls $HOME /remove "BUILTIN\\Users" /inheritance:r /grant:r "\`"NT AUTHORITY\\Authenticated Users\`":(CI)(OI)(RX)" &&
        New-Item -ItemType Directory -Path $HOME\\.ssh &&
        New-Item -ItemType File -Path $HOME\\.ssh\\authorized_keys &&
        Set-Content -Path $HOME\\.ssh\\authorized_keys -Value (Get-Content $HOME\\.ssh\\id_rsa.pub) &&
        icacls $HOME\\.ssh\\authorized_keys /inheritance:r /grant:r "\`"NT AUTHORITY\\Authenticated Users\`":(CI)(OI)(R)" &&
        New-Item -ItemType File -Path $HOME\\.ssh\\known_hosts &&
        ssh-keyscan -H localhost | Out-File -FilePath $HOME\\.ssh\\known_hosts &&
        icacls $HOME\\.ssh\\known_hosts /inheritance:r /grant:r "\`"NT AUTHORITY\\Authenticated Users\`":(CI)(OI)(R)" &&
        Start-Process ssh-agent &&
        ssh-add $HOME\\.ssh\\id_rsa`;
  } else {
    cmd = `chmod g-w $HOME                  &&
          chmod o-w $HOME                                 &&
          ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa        &&
          cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys &&
          chmod 0600 ~/.ssh/authorized_keys               &&
          ssh-keyscan -H localhost >> ~/.ssh/known_hosts  &&
          chmod 0600 ~/.ssh/known_hosts                   &&
          eval \`ssh-agent\`                              &&
          ssh-add ~/.ssh/id_rsa`;
  }

  exec(cmd, (err: any, stdout: any, stderr: any) => {
    core.info(stdout);
    core.warning(stderr);
    if (err) {
      core.error('Setup self ssh failed');
      throw new Error(err);
    }
  });

  // Start hdfs daemon.
  exec(
    `${hdfsHome}/bin/hdfs namenode -format`,
    (err: any, stdout: any, stderr: any) => {
      core.info(stdout);
      core.warning(stderr);
      if (err) {
        core.error('Format hdfs namenode failed');
        throw new Error(err);
      }
    }
  );

  exec(
    `${hdfsHome}/sbin/start-dfs.sh`,
    (err: any, stdout: any, stderr: any) => {
      core.info(stdout);
      core.warning(stderr);
      if (err) {
        core.error('Call start-dfs failed');
        throw new Error(err);
      }
    }
  );

  core.addPath(`${hdfsHome}/bin`);
  core.exportVariable('HDFS_NAMENODE_ADDR', '127.0.0.1:9000');
  core.exportVariable('HDFS_NAMENODE_HTTP_ADDR', '127.0.0.1:9870');
  core.exportVariable('HADOOP_HOME', hdfsHome);
}

setup().catch(err => {
  core.error(err);
  core.setFailed(err.message);
});
