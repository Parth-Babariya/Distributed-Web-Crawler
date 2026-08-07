const desiredHost = "mongodb:27017";

function sleep(ms) {
  const start = Date.now();
  while (Date.now() - start < ms) {
    // mongosh healthcheck helper; keep dependencies minimal.
  }
}

function ensureReplicaSetConfig() {
  try {
    const config = rs.conf();
    if (!config.members.some((member) => member.host === desiredHost)) {
      config.members[0].host = desiredHost;
      rs.reconfig(config, { force: true });
    }
  } catch (error) {
    rs.initiate({
      _id: "rs0",
      members: [{ _id: 0, host: desiredHost }],
    });
  }
}

ensureReplicaSetConfig();

for (let attempt = 0; attempt < 20; attempt += 1) {
  try {
    const status = rs.status();
    if (status.ok === 1 && status.members.some((member) => member.stateStr === "PRIMARY")) {
      quit(0);
    }
  } catch (error) {
    // Replica set may still be electing.
  }
  sleep(500);
}

quit(1);
