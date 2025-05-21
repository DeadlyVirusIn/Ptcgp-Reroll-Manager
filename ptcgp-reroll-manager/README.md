README.md - PTCGPRerollManager Python Version
Overview
This is a Python implementation of the PTCGP Reroll Manager, a Discord bot designed to work with Arturo's PTCG Bot for Pokémon TCG rerollers. It helps manage a community of players who are rerolling for rare cards and "God Packs" in the Pokémon Trading Card Game.
Features

User management with active/inactive/farm/leech states
Heartbeat monitoring for player activity
God Pack verification system
Statistics tracking for rerollers
Timeline statistics visualization
Godpack test tracking system
AntiCheat detection
Automatic inactive user management
Multiple language support (English/French)

Setup and Configuration

Install the required Python packages:

bashpip install discord.py aiohttp apscheduler matplotlib numpy

Update the config.py file with your Discord token and server information.
Run the bot:

bashpython main.py
Configuration Options
The bot is highly configurable through the config.py file. Key settings include:

Discord token and channel IDs
Auto-kick settings
GP verification settings
Leeching permissions
GP tracking intervals
Display settings
Language preferences

Commands
User Management

/setplayerid - Link your Discord account with your in-game ID
/setaverageinstances - Set your average number of game instances
/setprefix - Set your username prefix
/active - Set yourself as active
/inactive - Set yourself as inactive
/farm - Set yourself as a farmer (no main instance)
/leech - Set yourself as a leecher (only main instance)

Stats and Refresh

/refresh - Refresh user stats
/forcerefresh - Force refresh the IDs list
/misscount - Show miss statistics
/lastactivity - Show last heartbeat activity
/timelinestats - Display activity statistics over time
/refreshgplist - Refresh the GP tracking list

GP Verification

/verified - Mark a GP as verified
/dead - Mark a GP as dead/invalid
/liked - Mark a GP as liked
/notliked - Mark a GP as not liked
/miss - Report a miss test
/noshow - Report a test without showing the godpack
/resettest - Reset tests for a specific godpack
/testsummary - Display a summary of tests for a godpack

Utility

/generateusernames - Generate a list of usernames based on a prefix and keywords
/addgpfound - Add a GP to a user's stats (admin only)
/removegpfound - Remove a GP from a user's stats (admin only)

Directory Structure

main.py - Main bot code
config.py - Configuration settings
gp_test_utils.py - Utilities for GP test tracking
UserData.xml - User data storage
ServerData.xml - Server and GP data storage

Data Storage
The bot uses XML files for data storage:
UserData.xml
Stores information about users, including:

Player IDs
Activity status
Statistics (packs opened, GPs found)
Subsystem information
Heartbeat data

ServerData.xml
Stores information about GPs and server settings:

Eligible GPs
Live GPs
Ineligible (dead) GPs
GP verification status

Buttons Interface
The bot provides buttons in the commands channel for quick access to common actions:

Active (green) - Set yourself as active
Farm (blue) - Set yourself as a farmer
Leech (gray) - Set yourself as a leecher
Inactive (red) - Set yourself as inactive
Refresh Stats (gray) - Refresh user stats

Heartbeat System
The bot monitors user activity through a heartbeat system. Users need to configure their PTCG Bot to send heartbeat messages to the designated channel with the format:
DISCORD_ID
Online: instance1, instance2, ...
Packs: TIME PACKS
Select: PACK_TYPE
Type: ROLLING_TYPE
For secondary PCs or instances, use the format:
DISCORD_ID_PCNAME
GP Verification Process

When a God Pack is found, a thread is created in the appropriate verification forum
The thread starts with the ⌛ (waiting) status
Users can test the pack and report results:

/miss to report a failed test
/noshow to report a test without showing the GP
/verified to mark it as verified (live)
/liked to mark it good but not verified
/notliked to mark it as less likely to be live
/dead to mark it as dead/invalid



Timeline Statistics
The /timelinestats command generates visual statistics about user activity and pack opening rates over time, showing:

Active/Farm/Leech user counts
Packs opened
God Packs found
Growth trends

GP Testing System
The bot includes a sophisticated GP testing system to track the probability of a GP being "real" based on test results:

Each "no show" test reduces the probability based on slots/friends ratio
The /testsummary command shows detailed test statistics
The system helps the community optimize testing efficiency

Differences from Node.js Version
This Python version maintains all the core functionality of the original Node.js bot with some improvements:

More robust error handling
Enhanced visualization for timeline statistics
Simplified command registration process
Improved GP test tracking system
Better support for asynchronous operations

Requirements

Python 3.8+
discord.py 2.0+
matplotlib
numpy
aiohttp
apscheduler

Credits
This is a Python port of the original Node.js bot written by @thobi for the PTCGP Rerollers community. Special thanks to:

@Arturo-1212 for the PTCG Bot
@cjlj for Automated ids.txt modifications on the AHK side
The PTCGP Rerollers community for their support and feedback

License
GNU General Public License v3.0