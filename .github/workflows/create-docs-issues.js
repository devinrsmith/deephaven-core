module.exports = async ({ github, context }) => {
  const ISSUE_REPO_NAME = "deephaven.io";
  const ISSUE_TYPES = ["Community"];

  const body =
    `_This issue was auto-generated_\n\n` +
    `PR: ${context.payload.pull_request.html_url}\n` +
    `Author: ${context.payload.pull_request.user.login}\n\n` +
    "## Original PR Body\n\n" +
    context.payload.pull_request.body;

  const promises = ISSUE_TYPES.map((type) =>
    github.rest.issues.create({
      owner: context.repo.owner,
      repo: ISSUE_REPO_NAME,
      title: `${context.payload.pull_request.title}`,
      body,
      labels: [type, "documentation", "triage", "autogenerated"],
    })
  );

  const issues = await Promise.allSettled(promises);
  const issuesLinks = issues.map((promise, i) => {
    const type = ISSUE_TYPES[i];
    const { status, value, reason } = promise;
    const link =
      status === "fulfilled"
        ? value.data.html_url
        : `Failed to create issue: ${reason}`;
    return `${type[0].toUpperCase() + type.slice(1)}: ${link}`;
  });

  await github.rest.issues.createComment({
    issue_number: context.issue.number,
    owner: context.repo.owner,
    repo: context.repo.repo,
    body:
      "Labels indicate documentation is required. Issues for documentation have been opened:\n\n" +
      issuesLinks.join("\n"),
  });
};
