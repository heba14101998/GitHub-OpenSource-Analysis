"""
This module provides functions to scrape GitHub data, specifically focusing on 
contributors from a specified location (Egypt) and their associated repositories.

The script showcases an example workflow. It illustrates: 
  - Collect a dataset of contributors from Egypt (`scrap_egy_contributors`)
  - Collect Egyptiopn users (owners) repositories.
  - Collect top worldwide repos and display the Egyption contributors of these repositories.
"""

import csv
import pandas as pd
from tqdm import tqdm, gui
from typing import Optional, List, Dict
from urllib.parse import urljoin
from src.github_api import GitHubAPI

EGY_USERS_COLS = ["login", "url"]
REPO_COLS = ['login', 'repo_name', 'repo_html_url', 'language', 'topics',
             'repo_description', 'open_issues_count', 'forks_count',
             'stargazers_count', 'last_repo_commit_date', 'license']


class GittHubDataCollector(GitHubAPI):
    """
    Class for scraping contributors and their repositories from GitHub.
    Inherits from the GitHubAPI class for API interactions.
    """

    def __init__(self, ):
        super().__init__()

    def scrap_egy_contributors(
            self,
            users_file_path: str,
            start_page: int = 1) -> None:
        """
        Collect a dataset of contributors from Egypt (`scrap_egy_contributors`)

        This function iterates through pages of contributor data from the GitHub API,
        extracts relevant information (login, url), and saves it to a CSV file.
        """

        params = {
            "q": "location:egypt",
            "sort": "stars", 
            "order": "desc",
            "per_page": 100,
            "page": start_page
        }

        end_point = "https://api.github.com/search/users"

        with open(users_file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=EGY_USERS_COLS)
            writer.writeheader()

            page = start_page

            # with tqdm(total=estimated_pages, desc=f"Extracting Egyptian Github Users",  colour='green') as pbar:
            while True:
                params["page"] = page
                response = self._get(url=end_point, params=params)
                if response:
                
                    total_count = response["total_count"]
                    self.logger.info(f"Total contributors Found: {total_count}")

                    for item in tqdm(response["items"], total=100, desc=f"Extract Egyption Users in Page {page}",  colour='green'):
                        writer.writerow(
                            {k: v for k, v in item.items() if k in writer.fieldnames}
                        )
                        

                    self.logger.info(f"Scraping page number {page}")
                    page += 1

                    # Calculate the maximum page number to avoid going over
                    max_page = total_count // params["per_page"] + 1
                    # Once we've reached the last page 
                    if page > max_page:
                        self.logger.info("You Finshed all the pages!")
                        break
                else:
                    break

    def scrap_repos_file(
        self, users_file_path: str,
        repos_file_path: str,
        from_row: int = None
        ) -> None:
        """
        Scrapes repositories from the Egyption users' data.

        This function retrieves repository data associated with the collected GitHub Egyption users,
        extracts information like language, topics, and activity metrics, and saves it
        to a CSV file. It allows for skipping rows in the contributor CSV file for resuming
        the scraping process if needed.
        """

        with open(users_file_path, "r", newline="", encoding="utf-8") as users_file:
            reader = csv.DictReader(users_file)

            if from_row:
                # Skip rows before the start row
                for _ in range(from_row - 1):
                    next(reader)
             
            with open(repos_file_path, "w", newline="", encoding="utf-8") as repos_file:
                repos_writer = csv.DictWriter(repos_file, fieldnames=REPO_COLS)
                repos_writer.writeheader()
                
                with tqdm( desc="Scrap Egyption users repos", total = 50000,  colour='green') as pbar:                    
                    for i, row in enumerate(reader):
                        
                        # Extract all repos for spacific contributor
                        repo_url = row['url']+"/repos"
                        repos_response = self._get(repo_url)
                    
                        if repos_response is not None:

                            for repo in repos_response:
                                
                                repo_json = {
                                    "login": row["login"],
                                    "repo_name": repo["name"],
                                    "repo_html_url": repo["html_url"],
                                    "repo_description": repo["description"],
                                    "language": (
                                        "Python"
                                        if repo["language"] == "Jupyter Notebook"
                                        else repo["language"]
                                    ),
                                    "topics": ", ".join(repo["topics"]),
                                    "stargazers_count": repo["stargazers_count"],
                                    "forks_count": repo["forks_count"],
                                    "open_issues_count": repo["open_issues_count"],
                                    "last_repo_commit_date": repo["updated_at"],
                                    "license": (
                                        repo["license"]["name"]
                                        if "license" in repo and repo["license"]
                                        else None
                                    ),
                                }

                                repos_writer.writerow(repo_json)

                                pbar.update(1)

                                self.logger.info(f"scraped all `{row['login']}` repos")
                            
                            if i == 50000:
                                break
                        else:
                            self.logger.info(f"No repos for {row['login']}`")

    def scrap_non_egy_repos(
        self, top_non_egy_file_path: str = 'non_egy_repos.csv',
        start_page: int = 1,
        total_count: int = 100
        ):
        """
        Scrapes top GitHub repositories, excluding those in Egypt.

        This function iterates through pages of repository data from the GitHub API,
        extracts relevant information (owner, name, language, topics, etc.) and saves it to a CSV file.
        It handles rate limiting and potential errors during the scraping process.
        """

        params = {
            "q": "stars:>=1000", 
            "sort": "stars", 
            "order": "desc",
            "per_page": 100,
            "page": start_page,
        }
        end_point = "https://api.github.com/search/repositories"

        with open(top_non_egy_file_path, "w", newline="", encoding="utf-8") as file:

            writer = csv.DictWriter(file, fieldnames=REPO_COLS)
            writer.writeheader()

            page = start_page
            repos_scraped = 0
            
            # Estimate the total number of pages:
            total_pages = total_count // params['per_page'] + 1 
            
            with tqdm(total=500, desc="Scraping Top Non Egy Repos",  colour='green') as pbar:
                while repos_scraped < total_count:
                    response = self._get(url=end_point, params=params)

                    # if response is None:
                    #     self.logger.info("Error while scraping repositories")
                    #     break

                    if response:
                        self.logger.info(f"Total repositories Found: {response['total_count']}")

                        for item in response["items"]:
                            owner = item["owner"]["login"]
                            
                            repo_data = {
                                "login": owner,
                                "repo_name": item["name"],
                                "repo_html_url": item["html_url"],
                                "language": item.get("language"),
                                "topics": ", ".join(item.get("topics", [])),
                                "repo_description": item.get("description"),
                                "open_issues_count": item.get("open_issues_count"),
                                "forks_count": item.get("forks_count"),
                                "stargazers_count": item.get("stargazers_count"),
                                "last_repo_commit_date": item.get("updated_at"),
                                "license": (item.get("license", {}).get("name")  
                                            if item.get("license") else None)
                            }
                            writer.writerow(repo_data)

                            repos_scraped += 1 
                            if repos_scraped >= total_count: 
                                break
                            pbar.update(1)  # Update progress bar 
                            
                        self.logger.info(f"Scraping page number {page}")
                        page += 1
                    else:
                        break

    def filter_per_cont_loc(self, row) -> List[str]:
        """Filter repositories to find Egyptian contributors."""

        repo_owner = row['repo_html_url'].split("/")[-2]
        repo_name = row['repo_html_url'].split("/")[-1]
        repo_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
        contributors_url = f"{repo_url}/contributors"
        contributors = self._get(contributors_url)

        egyptian_contributors = []
        if contributors:
            # pbar.total = len(contributors)
            # # Reset progress bar to start for this repository
            # pbar.n = 0 
            # pbar.refresh()

            for cont in contributors: # tqdm(contributors, len(contributors), desc="Searching for Egyptions"):
                user_name = cont['login']
                repo_url = f"https://api.github.com/users/{user_name}"
                cont_prof = self._get(repo_url)

                location = cont_prof.get('location', '')
                if location and 'egypt' in location.lower():
                    self.logger.info(f"Find {user_name} contributes in {cont['repo_html_url']}")
                    egyptian_contributors.append(user_name)
            
            # pbar.update(1)  # Update the main progress bar
            return egyptian_contributors

        return None

# if __name__ == '__main__':

    # collector = GittHubDataCollector()
    # collector.scrap_egy_contributors("./data/egy_users.csv", start_page=1)
    # collector.scrap_repos_file("./data/egy_users.csv","./data/egy_users_repos1.csv") # , from_row=792
    # collector.scrap_non_egy_repos(
    #     "./data/scrap_non_egy_repos.csv",
    #     start_page=1, total_count=500)

    # df = pd.read_csv("./data/scrap_non_egy_repos.csv")
    # df = df.sample(100)

    # for index, row in tqdm(df.iterrows(), total=500, desc=f"Filter of only Egy contributors",  colour='green'):
    #     egy_conts = collector.filter_per_cont_loc(row)
    #     if egy_conts:
    #         df.loc[index, 'egyption_contributors'] = egy_conts
    #         df.to_csv("./data/top_egys.csv", index=False)