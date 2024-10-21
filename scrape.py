import requests
import pandas as pd
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from github import Github

# Load environment variables
load_dotenv()

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
g = Github(GITHUB_TOKEN)

# Function to clean up company names
def clean_company_name(company):
    if company:
        return company.strip().lstrip('@').upper()
    return company

# Function to fetch users from Mumbai with pagination
def fetch_users():
    url = 'https://api.github.com/search/users'
    users_data = []
    page = 1

    while True:
        params = {
            'q': 'location:Mumbai followers:>50',  # Query string for location and followers
            'per_page': 100,  # Maximum number of results per page
            'page': page
        }
        response = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, params=params)
        data = response.json()

        if 'items' not in data or not data['items']:
            break  # Exit if no more items are returned
        
        users_data.extend(data['items'])
        page += 1  # Increment to fetch the next page

    return users_data

# Function to fetch detailed user info and repos concurrently
def fetch_user_details(user):
    user_data = []
    repo_data = []
    
    try:
        user_details = g.get_user(user['login'])

        company_cleaned = clean_company_name(user_details.company)

        user_data = [
            user_details.login,
            user_details.name,
            company_cleaned,
            user_details.location,
            user_details.email,
            user_details.hireable,
            user_details.bio,
            user_details.public_repos,
            user_details.followers,
            user_details.following,
            user_details.created_at
        ]

        # Fetch up to 500 most recent repositories for each user
        repos = user_details.get_repos()
        for i, repo in enumerate(repos):
            if i >= 500:
                break  # Limit to 500 repos
            repo_data.append([
                user_details.login,
                repo.full_name,
                repo.created_at,
                repo.stargazers_count,
                repo.watchers_count,
                repo.language,
                repo.has_projects,
                repo.has_wiki,
                repo.license.name if repo.license else None
            ])
    
    except Exception as e:
        print(f"Error fetching data for {user['login']}: {e}")
    
    return user_data, repo_data

# Main function to run the scraping
def main():
    # Fetch users in Mumbai with > 50 followers
    users_data = fetch_users()
    
    user_list = []
    repo_list = []

    # Use ThreadPoolExecutor to fetch user details concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_user_details, users_data))

        for user_data, repo_data in results:
            if user_data:
                user_list.append(user_data)
            if repo_data:
                repo_list.extend(repo_data)

    # Create CSV for users
    users_df = pd.DataFrame(user_list, columns=[
        'login', 'name', 'company', 'location', 'email', 'hireable', 
        'bio', 'public_repos', 'followers', 'following', 'created_at'])

    users_df.to_csv('users.csv', index=False)

    # Create CSV for repositories
    repos_df = pd.DataFrame(repo_list, columns=[
        'login', 'full_name', 'created_at', 'stargazers_count', 
        'watchers_count', 'language', 'has_projects', 'has_wiki', 'license_name'])

    repos_df.to_csv('repositories.csv', index=False)

    print("CSV files created successfully!")

if __name__ == "__main__":
    main()
