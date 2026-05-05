To configure a repo for webhook triggers, create:
configs/webhooks/<github-org>/<repo-name>/.drev.yml

Example for github.com/octocat/hello-world:
configs/webhooks/octocat/hello-world/.drev.yml

Then in GitHub:
Settings → Webhooks → Add webhook
  Payload URL: http://your-server/webhooks/github
  Content type: application/json
  Secret: <your webhook secret>
  Events: Just the push event
