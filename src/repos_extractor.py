"""
This module relies on a base `GitHubAPI` class.
The module extracts detailed information about GitHub repositories and analyzes various aspects such as:

- Files
- Contributors
- Commits
- Issues
- Pull Requests
- Tags
- CI/CD Tools

"""

import urllib
# from datetime import datetime
from typing import Optional, Dict, List

from src.github_api import GitHubAPI


class GitHubRepoExtractor(GitHubAPI):
    """
    This class extracts data about GitHub repositories, processing information
    like file structure, contributors, and activity metrics. It uses the
    `GitHubAPI` class for communication with the GitHub API.
    """

    def __init__(self, row: Dict):

        super().__init__()

        self.row = row
        repo_owner = self.row['repo_html_url'].split("/")[-2]
        repo_name = self.row['repo_html_url'].split("/")[-1]
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"

        # self.last_year = datetime.datetime.now() - datetime.timedelta(days=365)
        # self.last_year = self.last_year.isoformat()

    def _get_filenames(self) -> List[str]:
        """Recursively retrieves all files and directories within a repository."""

        def _fetch_recursive(directory: str) -> List[str]:
            """Helper function to recursively get files/directories."""
            # Replace reserved characters with their encoded equivalents
            # Example:
            # - "MyFile.cs#" becomes "MyFile.cs%23" (as %23 is # encoded for URL)
            # - "MyFile&More.cs" becomes "MyFile%26More.cs"
            # This ensures proper URL encoding for filenames containing special
            # characters

            directory = urllib.parse.quote(directory)
            contents_url = f"{self.base_url}/contents/{directory}"
            data = self._get(contents_url)

            if data:
                for item in data:
                    # Don't retraive imamges and backup python files
                    if item["type"] == "file" and not item["name"].lower().endswith(
                            (".pyc", ".png", ".jpg", ".jpeg", ".gif")):

                        yield item["path"]

                    elif item["type"] == "dir" and item["name"].lower() not in ['images', 'imgs', '__pycache__']:
                        yield from _fetch_recursive(item["path"])

            else:
                self.logger.warning(
                    f"Failed to retrieve content for directory: {directory}")

        return list(_fetch_recursive(""))

    def _get_contributors_count(self) -> Optional[int]:
        """Fetches the number of contributors for a given repository."""

        contributors_url = f"{self.base_url}/contributors"
        data = self._get(contributors_url)

        if data:
            self.logger.info(f"Contributor count: {len(data)}")
            return len(data)

        return None

    def _get_commits_count(self) -> int:
        """Fetches the number of commits for a given repository."""

        commits_url = f"{self.base_url}/commits"
        data = self._get(commits_url)

        if data:
            self.logger.info(f"Commit count: {len(data)}")
            return len(data)

        return 0

    def _get_issues_count(self) -> Optional[Dict[str, int]]:
        """Fetches open and closed issues counts from a GitHub repository."""

        issues_url = f"{self.base_url}/issues"
        data = self._get(issues_url, params={"state": "all"})

        if data:

            open_count = sum(1 for issue in data if issue["state"] == "open")
            closed_count = sum(
                1 for issue in data if issue["state"] == "closed")

            self.logger.info(
                f"Issue counts: open - {open_count}, closed - {closed_count}")

            return {"open": open_count, "closed": closed_count}

        return None

    def _get_pull_requests_count(self) -> Optional[Dict[str, int]]:
        """Fetches open and closed pull request counts from a GitHub repository."""

        pulls_url = f"{self.base_url}/pulls"
        data = self._get(pulls_url, params={"state": "all"})

        if data:

            open_count = sum(
                1 for pull_request in data if pull_request["state"] == "open")
            closed_count = sum(
                1 for pull_request in data if pull_request["state"] == "closed")
            merged_count = sum(
                1 for pull_request in data if pull_request["merged_at"] is not None)

            self.logger.info(
                f"Pull request counts: open - {open_count},"
                f"closed - {closed_count}, merged - {merged_count}"
            )

            return {
                "open": open_count,
                "closed": closed_count,
                "merged": merged_count}

        return None

    def _get_tags(self) -> Optional[List[str]]:
        """Fetches tags list for a specific repository."""

        tags_url = f"{self.base_url}/tags"
        data = self._get(tags_url)

        if data:
            tags = [tag["name"] for tag in data]
            self.logger.info(f"Tags: {tags}")
            return tags

        return None

    def _get_dependencies(self) -> Optional[str]:
        """ Fetches dependency information using the GitHub dependency graph API """

        dependencies_url = f"{self.base_url}/dependency-graph/sbom"
        data = self._get(dependencies_url)

        if data:

            packages = [package["name"]
                        for package in data["sbom"]["packages"]]
            packages_names = [pkg.split(':')[-1] for pkg in packages]

            self.logger.info(f"Packages: {packages_names}")

            return packages_names[1:]

        return None

    def _get_ci_cd_tool(self) -> Optional[str]:
        """ Extracts information about CI/CD pipelines by checking for configuration files. """

        for filename in self.row["filenames"]:

            if filename.lower() == ".travis.yml":
                return "Travis CI"
            elif filename.lower() == ".gitlab-ci.yml":
                return "GitLab CI"
            elif filename.lower() == ".drone.yml":
                return "Drone CI"
            elif filename.lower() == ".circleci/config.yml":
                return "CircleCI"
            elif filename.lower() == ".github/workflows":
                return "GitHub Actions"
            elif filename.lower() == "Jenkinsfile":
                return "Jenkins"
            else:
                return None

    def process_repo(self) -> Dict:
        """Process a single repo (single row) from the dataframe"""

        self.row['contributor_count'] = self._get_contributors_count()

        if (self.row['contributor_count'] is not None) and (self.row['contributor_count'] > 1):
        # (self.row['last_repo_commit_date'] is not None) and\
        # (self.row['stargazers_count'] is not None) and\
        # (self.row['stargazers_count'] > 3) and \
        # (self.row['last_repo_commit_date'] >= self.last_year) and \
            self.row['filenames'] = self._get_filenames()
            self.row['commits_count'] = self._get_commits_count()
            self.row['issues_count'] = self._get_issues_count()
            self.row['pull_requests_count'] = self._get_pull_requests_count()
            self.row['tags'] = self._get_tags()
            self.row['dependencies'] = self._get_dependencies()
            self.row['ci_cd_tool'] = self._get_ci_cd_tool()

            self.logger.info(f"\n{self.row}")
        else:
            self.logger.info(f"Excluded based on out criteria")

        return self.row


# if __name__ == '__main__':

#     import pandas as pd
#     from tqdm import tqdm

#     df = pd.read_csv("./data/repos.csv").sample(2)
#     new_data = []

#     for index, row in tqdm(df.iterrows(), total=len(df),
#                            desc="Extracting Repos Details"):
#         extractor = GitHubRepoExtractor(row)
#         new_data.append(extractor.process_repo())

#     new_df = pd.DataFrame(new_data)
#     new_df.to_csv("./data/extractor_out.csv", index=False)
