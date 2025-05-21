import aiohttp
import logging
import config

logger = logging.getLogger("bot")

async def update_gist(git_content: str, git_name: str = config.git_gist_group_name) -> None:
    """
    Update a GitHub Gist with new content.
    
    Args:
        git_content: The content to upload to the Gist
        git_name: The name of the Gist file (defaults to config.git_gist_group_name)
    """
    if not git_content:
        git_content = "empty"
        
    try:
        # Extract the real Gist ID from the URL
        gist_id = config.git_gist_id.split('/')[-2]
        
        # Prepare headers and data
        headers = {
            "Authorization": f"token {config.git_token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        
        data = {
            "files": {
                git_name: {
                    "content": git_content
                }
            }
        }
        
        # Use aiohttp to make the API request
        async with aiohttp.ClientSession() as session:
            url = f"https://api.github.com/gists/{gist_id}"
            async with session.patch(url, headers=headers, json=data) as response:
                if response.status == 200:
                    logger.info(f"🌐 Successfully uploaded to GitGist - {git_name}")
                else:
                    error_text = await response.text()
                    logger.error(f"❌ ERROR uploading to GitGist - {git_name}: {response.status} - {error_text}")
    except Exception as e:
        logger.error(f"❌ ERROR trying to upload to GitGist - {git_name}: {e}")