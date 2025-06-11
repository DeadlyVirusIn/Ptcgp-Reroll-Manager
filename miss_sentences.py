import discord
import random
from typing import List, Optional, Union

# MISS REACTION SENTENCES
# These are used when a user misses a reroll opportunity

MISS_SENTENCES = [
    "Oops! You missed that one! 😅",
    "Better luck next time! 🍀",
    "Close, but not quite! 🎯",
    "Keep trying, you've got this! 💪",
    "Almost had it! 🤏",
    "Miss! But practice makes perfect! 📈",
    "Whoosh! That one got away! 💨",
    "Not this time, but keep going! 🚀",
    "Swing and a miss! ⚾",
    "You'll catch the next one! 🎣",
    "Missed it by that much! 📏",
    "Keep your eye on the prize! 👁️",
    "The next one is yours! 🎪",
    "Gotta be quicker than that! ⚡",
    "So close, yet so far! 🌟",
    "Don't give up now! 🔥",
    "That's what we call a learning experience! 📚",
    "Round two, fight! 🥊",
    "Shake it off and try again! 🎭",
    "Every expert was once a beginner! 🎓"
]

# ENCOURAGEMENT SENTENCES
# These are used to encourage users who are struggling

ENCOURAGEMENT_SENTENCES = [
    "You're doing great! Keep it up! 🌟",
    "Every miss is one step closer to success! 📈",
    "Your persistence will pay off! 💎",
    "Believe in yourself! 🌈",
    "You're learning and improving! 🎯",
    "Don't give up, champion! 👑",
    "Your dedication is inspiring! ✨",
    "Progress, not perfection! 📊",
    "You're stronger than you think! 💪",
    "Keep pushing forward! 🚀",
    "Success is just around the corner! 🎪",
    "You've got the right spirit! 🔥",
    "Every attempt makes you better! 📚",
    "Your hard work will pay off! 💰",
    "Stay focused on your goal! 🎯",
    "You're on the right track! 🛤️",
    "Consistency is key! 🔑",
    "Your time will come! ⏰",
    "Keep that positive energy! ⚡",
    "You're closer than you think! 🎈"
]

# SUCCESS CELEBRATION SENTENCES
# These are used when a user successfully gets a good reroll

SUCCESS_SENTENCES = [
    "Excellent work! 🎉",
    "Perfect timing! ⏰",
    "You nailed it! 🎯",
    "Fantastic job! 🌟",
    "That's what I'm talking about! 🔥",
    "Brilliant execution! ✨",
    "You're on fire! 🚀",
    "Outstanding performance! 👏",
    "That's the spirit! 💪",
    "Incredible work! 🎊",
    "You've got the magic touch! ✨",
    "Perfection achieved! 💎",
    "That's how it's done! 🎪",
    "Masterful! 🏆",
    "You're unstoppable! ⚡",
    "Simply amazing! 🌈",
    "Top tier performance! 👑",
    "You make it look easy! 😎",
    "Flawless victory! 🥇",
    "Keep that momentum going! 🌪️"
]

# GODPACK CELEBRATION SENTENCES
# Special sentences for when someone gets a godpack

GODPACK_SENTENCES = [
    "🎉 GODPACK ACHIEVED! 🎉",
    "⚡ LEGENDARY PULL! ⚡",
    "🌟 ABSOLUTE PERFECTION! 🌟",
    "🔥 YOU'RE ON FIRE! 🔥",
    "💎 DIAMOND TIER LUCK! 💎",
    "👑 PACK ROYALTY! 👑",
    "🚀 TO THE MOON! 🚀",
    "✨ PURE MAGIC! ✨",
    "🏆 CHAMPION STATUS! 🏆",
    "🎯 BULLSEYE! 🎯",
    "🌈 RAINBOW LUCK! 🌈",
    "⭐ SUPERSTAR MOMENT! ⭐",
    "🎪 SPECTACULAR! 🎪",
    "💫 COSMIC LUCK! 💫",
    "🎊 CELEBRATION TIME! 🎊"
]

# MOTIVATIONAL QUOTES
# Longer motivational messages for specific situations

MOTIVATIONAL_QUOTES = [
    "Remember: Every master was once a disaster! 🎭",
    "The only way to do great work is to love what you do! 💖",
    "Success is not final, failure is not fatal! 💪",
    "Your only limit is your mind! 🧠",
    "Great things never come from comfort zones! 🚀",
    "Dream big, work hard, stay focused! 🎯",
    "Progress is impossible without change! 📈",
    "Champions keep playing until they get it right! 🏆",
    "The expert in anything was once a beginner! 🌱",
    "Your potential is endless! ♾️"
]

# FARMING ENCOURAGEMENT
# Messages for users who are farming for long periods

FARMING_SENTENCES = [
    "Keep grinding, farmer! 🚜",
    "Your dedication is paying off! 🌾",
    "Harvest time is coming! 🌽",
    "Plant those seeds of effort! 🌱",
    "The farm never sleeps! 🌙",
    "Cultivating greatness! 🌿",
    "Your crops are growing! 📈",
    "Patience yields the best harvest! ⏳",
    "From seed to success! 🌳",
    "The grind never stops! ⚙️",
    "Farming like a pro! 👨‍🌾",
    "Seasonal dedication! 🍂",
    "Growing your empire! 🏰",
    "Fields of opportunity! 🌻",
    "The early farmer gets the best crops! 🌅"
]

# WARNING MESSAGES
# Messages for when users need to be careful or slow down

WARNING_SENTENCES = [
    "⚠️ Take it easy there! ⚠️",
    "🚨 Slow down a bit! 🚨",
    "⏳ Patience is a virtue! ⏳",
    "🛑 Easy does it! 🛑",
    "⚡ Cool your jets! ⚡",
    "🎯 Focus on quality over quantity! 🎯",
    "🐌 Slow and steady wins the race! 🐌",
    "⌛ Good things come to those who wait! ⌛",
    "🚦 Yellow light - proceed with caution! 🚦",
    "🧘 Take a deep breath! 🧘"
]

# MILESTONE CELEBRATION SENTENCES
# Messages for reaching certain milestones

MILESTONE_SENTENCES = [
    "🎯 Milestone achieved! 🎯",
    "📈 Level up! 📈",
    "🏅 Badge earned! 🏅",
    "⭐ New personal record! ⭐",
    "🎊 Celebration worthy! 🎊",
    "🔓 Achievement unlocked! 🔓",
    "📊 Stats are looking good! 📊",
    "🎪 Show stopper performance! 🎪",
    "💯 Century club! 💯",
    "🚀 Blast off to new heights! 🚀"
]

# STREAK SENTENCES
# Messages for maintaining streaks

STREAK_SENTENCES = [
    "🔥 Streak maintained! 🔥",
    "⚡ Lightning streak! ⚡",
    "🌟 Consistency champion! 🌟",
    "📈 Upward trajectory! 📈",
    "🎯 On target! 🎯",
    "💪 Strength in consistency! 💪",
    "🏆 Streak legend! 🏆",
    "⭐ Star performer! ⭐",
    "🔥 Hot streak! 🔥",
    "💎 Diamond consistency! 💎"
]

# COMEBACK SENTENCES
# Messages for users making a comeback after struggles

COMEBACK_SENTENCES = [
    "🔄 What a comeback! 🔄",
    "📈 Rising from the ashes! 📈",
    "⚡ Lightning recovery! ⚡",
    "🌅 New dawn, new day! 🌅",
    "🚀 Bouncing back! 🚀",
    "💪 Resilience at its finest! 💪",
    "🎯 Back on target! 🎯",
    "🌟 Shining bright again! 🌟",
    "🔥 Phoenix rising! 🔥",
    "👑 Return of the king! 👑"
]

# UTILITY FUNCTIONS FOR MANAGING SENTENCES

def get_random_miss_sentence() -> str:
    """Get a random miss reaction sentence."""
    try:
        return random.choice(MISS_SENTENCES)
    except (IndexError, AttributeError):
        return "Better luck next time! 🍀"

def get_random_encouragement() -> str:
    """Get a random encouragement sentence."""
    try:
        return random.choice(ENCOURAGEMENT_SENTENCES)
    except (IndexError, AttributeError):
        return "Keep going! You've got this! 💪"

def get_random_success_sentence() -> str:
    """Get a random success celebration sentence."""
    try:
        return random.choice(SUCCESS_SENTENCES)
    except (IndexError, AttributeError):
        return "Great job! 🎉"

def get_random_godpack_sentence() -> str:
    """Get a random godpack celebration sentence."""
    try:
        return random.choice(GODPACK_SENTENCES)
    except (IndexError, AttributeError):
        return "🎉 GODPACK ACHIEVED! 🎉"

def get_random_motivational_quote() -> str:
    """Get a random motivational quote."""
    try:
        return random.choice(MOTIVATIONAL_QUOTES)
    except (IndexError, AttributeError):
        return "Your potential is endless! ♾️"

def get_random_farming_sentence() -> str:
    """Get a random farming encouragement sentence."""
    try:
        return random.choice(FARMING_SENTENCES)
    except (IndexError, AttributeError):
        return "Keep grinding! 🚜"

def get_random_warning_sentence() -> str:
    """Get a random warning sentence."""
    try:
        return random.choice(WARNING_SENTENCES)
    except (IndexError, AttributeError):
        return "⚠️ Take it easy! ⚠️"

def get_random_milestone_sentence() -> str:
    """Get a random milestone celebration sentence."""
    try:
        return random.choice(MILESTONE_SENTENCES)
    except (IndexError, AttributeError):
        return "🎯 Milestone achieved! 🎯"

def get_random_streak_sentence() -> str:
    """Get a random streak sentence."""
    try:
        return random.choice(STREAK_SENTENCES)
    except (IndexError, AttributeError):
        return "🔥 Streak maintained! 🔥"

def get_random_comeback_sentence() -> str:
    """Get a random comeback sentence."""
    try:
        return random.choice(COMEBACK_SENTENCES)
    except (IndexError, AttributeError):
        return "🔄 What a comeback! 🔄"

def get_contextual_message(context: str, **kwargs) -> str:
    """Get a contextual message based on the situation."""
    try:
        context_lower = context.lower()
        
        if context_lower in ['miss', 'missed', 'fail']:
            return get_random_miss_sentence()
        elif context_lower in ['success', 'hit', 'good']:
            return get_random_success_sentence()
        elif context_lower in ['godpack', 'gp', 'legendary']:
            return get_random_godpack_sentence()
        elif context_lower in ['encourage', 'motivate', 'cheer']:
            return get_random_encouragement()
        elif context_lower in ['farm', 'farming', 'grind']:
            return get_random_farming_sentence()
        elif context_lower in ['warning', 'warn', 'slow']:
            return get_random_warning_sentence()
        elif context_lower in ['milestone', 'achievement', 'goal']:
            return get_random_milestone_sentence()
        elif context_lower in ['streak', 'consistent', 'chain']:
            return get_random_streak_sentence()
        elif context_lower in ['comeback', 'recovery', 'return']:
            return get_random_comeback_sentence()
        else:
            return get_random_encouragement()
            
    except Exception as e:
        print(f"❌ Error getting contextual message: {e}")
        return "Keep going! 💪"

def get_personalized_message(username: str, context: str, **kwargs) -> str:
    """Get a personalized message for a specific user."""
    try:
        base_message = get_contextual_message(context, **kwargs)
        
        # Add personalization if username is provided
        if username and len(username.strip()) > 0:
            return f"{username}, {base_message.lower()}"
        else:
            return base_message
            
    except Exception as e:
        print(f"❌ Error getting personalized message: {e}")
        return "Keep it up! 🌟"

def find_emoji(bot, emoji_name: str, fallback: str = "❓") -> str:
    """Find custom emoji by name, return fallback if not found."""
    try:
        if not bot or not hasattr(bot, 'guilds'):
            return fallback
            
        # Search through all guilds the bot is in
        for guild in bot.guilds:
            # Get emoji by name
            emoji = discord.utils.get(guild.emojis, name=emoji_name)
            if emoji:
                return str(emoji)
                
        # If not found, return fallback
        return fallback
        
    except Exception as e:
        print(f"❌ Error finding emoji '{emoji_name}': {e}")
        return fallback

def get_emoji_by_id(bot, emoji_id: int, fallback: str = "❓") -> str:
    """Get emoji by ID, return fallback if not found."""
    try:
        if not bot:
            return fallback
            
        emoji = bot.get_emoji(emoji_id)
        return str(emoji) if emoji else fallback
        
    except Exception as e:
        print(f"❌ Error getting emoji by ID {emoji_id}: {e}")
        return fallback

def create_reaction_embed(title: str, message: str, color: int = 0x3498db) -> discord.Embed:
    """Create a reaction embed with title and message."""
    try:
        embed = discord.Embed(
            title=title,
            description=message,
            color=color
        )
        embed.timestamp = discord.utils.utcnow()
        return embed
    except Exception as e:
        print(f"❌ Error creating reaction embed: {e}")
        # Return a basic embed as fallback
        return discord.Embed(description=message, color=0x3498db)

def format_miss_message(username: str, miss_count: int, total_attempts: int) -> str:
    """Format a miss message with statistics."""
    try:
        base_message = get_random_miss_sentence()
        
        if total_attempts > 0:
            success_rate = round(((total_attempts - miss_count) / total_attempts) * 100, 1)
            stats = f"Miss #{miss_count} | Success Rate: {success_rate}%"
        else:
            stats = f"Miss #{miss_count}"
            
        return f"{username}, {base_message}\n*{stats}*"
        
    except Exception as e:
        print(f"❌ Error formatting miss message: {e}")
        return f"{username}, better luck next time!"

def format_success_message(username: str, success_count: int, total_attempts: int) -> str:
    """Format a success message with statistics."""
    try:
        base_message = get_random_success_sentence()
        
        if total_attempts > 0:
            success_rate = round((success_count / total_attempts) * 100, 1)
            stats = f"Success #{success_count} | Success Rate: {success_rate}%"
        else:
            stats = f"Success #{success_count}"
            
        return f"{username}, {base_message}\n*{stats}*"
        
    except Exception as e:
        print(f"❌ Error formatting success message: {e}")
        return f"{username}, excellent work!"

def get_streak_celebration(streak_count: int) -> str:
    """Get celebration message based on streak count."""
    try:
        if streak_count >= 100:
            return f"🔥 INCREDIBLE {streak_count} STREAK! 🔥"
        elif streak_count >= 50:
            return f"⚡ AMAZING {streak_count} STREAK! ⚡"
        elif streak_count >= 25:
            return f"🌟 FANTASTIC {streak_count} STREAK! 🌟"
        elif streak_count >= 10:
            return f"💪 GREAT {streak_count} STREAK! 💪"
        elif streak_count >= 5:
            return f"🎯 NICE {streak_count} STREAK! 🎯"
        else:
            return f"📈 {streak_count} in a row! 📈"
            
    except Exception as e:
        print(f"❌ Error getting streak celebration: {e}")
        return "🔥 Nice streak! 🔥"

def get_milestone_celebration(milestone_type: str, value: int) -> str:
    """Get celebration message for reaching milestones."""
    try:
        milestone_messages = {
            'packs': f"📦 {value:,} packs opened! 📦",
            'hours': f"⏰ {value} hours played! ⏰",
            'days': f"📅 {value} days active! 📅",
            'godpacks': f"✨ {value} godpacks found! ✨",
            'level': f"📈 Level {value} reached! 📈"
        }
        
        return milestone_messages.get(milestone_type.lower(), f"🎯 {value} achieved! 🎯")
        
    except Exception as e:
        print(f"❌ Error getting milestone celebration: {e}")
        return "🎉 Milestone reached! 🎉"

# SENTENCE MANAGEMENT FUNCTIONS

def add_custom_sentence(category: str, sentence: str) -> bool:
    """Add a custom sentence to a category."""
    try:
        category_map = {
            'miss': MISS_SENTENCES,
            'encouragement': ENCOURAGEMENT_SENTENCES,
            'success': SUCCESS_SENTENCES,
            'godpack': GODPACK_SENTENCES,
            'motivational': MOTIVATIONAL_QUOTES,
            'farming': FARMING_SENTENCES,
            'warning': WARNING_SENTENCES,
            'milestone': MILESTONE_SENTENCES,
            'streak': STREAK_SENTENCES,
            'comeback': COMEBACK_SENTENCES
        }
        
        target_list = category_map.get(category.lower())
        if target_list is not None and sentence not in target_list:
            target_list.append(sentence)
            return True
        return False
        
    except Exception as e:
        print(f"❌ Error adding custom sentence: {e}")
        return False

def remove_custom_sentence(category: str, sentence: str) -> bool:
    """Remove a custom sentence from a category."""
    try:
        category_map = {
            'miss': MISS_SENTENCES,
            'encouragement': ENCOURAGEMENT_SENTENCES,
            'success': SUCCESS_SENTENCES,
            'godpack': GODPACK_SENTENCES,
            'motivational': MOTIVATIONAL_QUOTES,
            'farming': FARMING_SENTENCES,
            'warning': WARNING_SENTENCES,
            'milestone': MILESTONE_SENTENCES,
            'streak': STREAK_SENTENCES,
            'comeback': COMEBACK_SENTENCES
        }
        
        target_list = category_map.get(category.lower())
        if target_list is not None and sentence in target_list:
            target_list.remove(sentence)
            return True
        return False
        
    except Exception as e:
        print(f"❌ Error removing custom sentence: {e}")
        return False

def get_sentence_count(category: str) -> int:
    """Get the number of sentences in a category."""
    try:
        category_map = {
            'miss': MISS_SENTENCES,
            'encouragement': ENCOURAGEMENT_SENTENCES,
            'success': SUCCESS_SENTENCES,
            'godpack': GODPACK_SENTENCES,
            'motivational': MOTIVATIONAL_QUOTES,
            'farming': FARMING_SENTENCES,
            'warning': WARNING_SENTENCES,
            'milestone': MILESTONE_SENTENCES,
            'streak': STREAK_SENTENCES,
            'comeback': COMEBACK_SENTENCES
        }
        
        target_list = category_map.get(category.lower())
        return len(target_list) if target_list is not None else 0
        
    except Exception as e:
        print(f"❌ Error getting sentence count: {e}")
        return 0

def get_all_categories() -> List[str]:
    """Get list of all available sentence categories."""
    return [
        'miss', 'encouragement', 'success', 'godpack', 'motivational',
        'farming', 'warning', 'milestone', 'streak', 'comeback'
    ]

# EXPORT ALL FUNCTIONS
__all__ = [
    # Sentence lists
    'MISS_SENTENCES', 'ENCOURAGEMENT_SENTENCES', 'SUCCESS_SENTENCES',
    'GODPACK_SENTENCES', 'MOTIVATIONAL_QUOTES', 'FARMING_SENTENCES',
    'WARNING_SENTENCES', 'MILESTONE_SENTENCES', 'STREAK_SENTENCES',
    'COMEBACK_SENTENCES',
    
    # Random sentence getters
    'get_random_miss_sentence', 'get_random_encouragement', 'get_random_success_sentence',
    'get_random_godpack_sentence', 'get_random_motivational_quote', 'get_random_farming_sentence',
    'get_random_warning_sentence', 'get_random_milestone_sentence', 'get_random_streak_sentence',
    'get_random_comeback_sentence',
    
    # Contextual and personalized messages
    'get_contextual_message', 'get_personalized_message',
    
    # Emoji functions
    'find_emoji', 'get_emoji_by_id',
    
    # Embed and formatting
    'create_reaction_embed', 'format_miss_message', 'format_success_message',
    
    # Celebrations
    'get_streak_celebration', 'get_milestone_celebration',
    
    # Management functions
    'add_custom_sentence', 'remove_custom_sentence', 'get_sentence_count', 'get_all_categories'
]

print("✅ Enhanced miss_sentences.py loaded successfully")