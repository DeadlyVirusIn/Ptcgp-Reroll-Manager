import discord
import random
from typing import Optional, List, Union

def localize(fr: str, en: str) -> str:
    """Return localized text based on language setting."""
    from config import english_language
    return en if english_language else fr

def get_random_string_from_array(array: List[str]) -> str:
    """Get a random string from an array."""
    return random.choice(array)

# Emoji placeholders
emoji_Copium = ""
emoji_Bedge = ""
emoji_KEKW = ""
emoji_KEKWait = ""
emoji_OkaygeBusiness = ""
emoji_PeepoClown = ""
emoji_Prayge = ""
emoji_Sadge = ""

def find_emoji(client: discord.Client, name: str, fallback: str = "") -> str:
    """Find emoji by name or ID, return fallback if not found."""
    # First try by name
    emoji = discord.utils.get(client.emojis, name=name)
    
    # If not found, try by ID (if name looks like an ID)
    if not emoji and name.isdigit():
        emoji = client.get_emoji(int(name))
    
    if not emoji:
        return fallback
    
    return str(emoji)

def init_emojis(client: discord.Client) -> None:
    """Initialize emoji variables."""
    global emoji_Copium, emoji_Bedge, emoji_KEKW, emoji_KEKWait
    global emoji_OkaygeBusiness, emoji_PeepoClown, emoji_Prayge, emoji_Sadge
    
    emoji_Copium = find_emoji(client, 'Copium')
    emoji_Bedge = find_emoji(client, 'Bedge')
    emoji_KEKW = find_emoji(client, 'KEKW')
    emoji_KEKWait = find_emoji(client, 'KEKWait')
    emoji_OkaygeBusiness = find_emoji(client, 'OkaygeBusiness')
    emoji_PeepoClown = find_emoji(client, 'PeepoClown')
    emoji_Prayge = find_emoji(client, 'Prayge')
    emoji_Sadge = find_emoji(client, 'Sadge')

def get_low_tension_message() -> str:
    """Get a random low tension miss message."""
    # Using global emojis - init_emojis() should be called before this
    return localize(
        get_random_string_from_array([
            f"Tranquille c\'est le début, on y croit",
            f"C\'est rien c\'est rien il arrive après le GP",
            f"C\'est que le début",
            f"10% c\'est 100% {emoji_Bedge}",
            f"De toute manière il est pourri ce GP {emoji_Bedge}",
            f"Je suis sur que t'as déjà toutes les 2star {emoji_Bedge}",
            f"T\'as pas le droit de nous faire perdre espoir comme ca...",
            f"MAIS C\'ÉTAIT SUR EN FAIT, C\'ÉTAIT SUUUUR",
            f"Pas de bras, pas de chocolat {emoji_KEKWait}",
            f"Avoir un gp, c\'est comme essayer de faire 3 fois face avec Ondine, impossible. {emoji_KEKWait}",
            f"La légende raconte que quelqu\'un, quelque part, a déjà vu un God Pack... Mais pas toi {emoji_KEKW}",
            f"Raté... C'est comme chercher un Shiny sans Charme Chroma {emoji_Sadge}",
            f"Ce serait pas un gp dud ca comme même ?",
            f"Aïe aïe aïe",
            f"Y\'a R {emoji_Copium}"
        ]),
        get_random_string_from_array([
            f"It\'s fine, we just started",
            f"10% = 100% {emoji_Bedge}",
            f"Nah we\'re good, the gp is on his way {emoji_Copium}"
        ])
    )

def get_medium_tension_message() -> str:
    """Get a random medium tension miss message."""
    return localize(
        get_random_string_from_array([
            f"Il commence à y avoir une petite odeur la non {emoji_KEKWait}?",
            f"C\'est terrible ce qui se passe {emoji_Sadge}",
            f"Petit {emoji_Prayge} et ca passe tranquille",
            f"Plus rien ne va... {emoji_Sadge}",
            f"Si c\'est vraiment dead on vire l\'host en même temps que son pack {emoji_KEKW}",
            f"Qu\'est-ce qu\'on t\'a fait pour mériter ça {emoji_KEKWait}",
            f"Oh Bonne Mère j'espère que c'est le dernier /miss celui la putaing cong",
            f"Attendre d'avoir un gp alive, c'est comme attendre avec l'envie de caguer sans jamais pouvoir y aller {emoji_KEKWait}",
            f"À ce rythme, tu vas écrire un livre : 1001 façons de ne PAS choper un God Pack {emoji_PeepoClown}",
            f"Moi j\'y crois encore tkt {emoji_Copium}"
        ]),
        get_random_string_from_array([
            f"Forget about it, next one is GP {emoji_Prayge}",
            f"Damn that stinks for sure"
        ])
    )

def get_high_tension_message() -> str:
    """Get a random high tension miss message."""
    return localize(
        get_random_string_from_array([
            f"OOF {emoji_Sadge}",
            f"UUUUUUUUUUUUUUUUUUSTRE",
            f"Un GP de moins ici c\'est du karma en plus {emoji_OkaygeBusiness}",
            f"Ca fait beaucoup là, non... ?",
            f"TU ES TILTÉ BOUBOU ! TU AS BESOIN DE BOL ! {emoji_KEKW}",
            f"T'as mieux fait de perdre celui la que de rater le pick d'un 4/5",
            f"C'est ciao.",
            f"Oh pinaise un gp invalide Marge file moi un donut sucré au sucre",
            f"Tout espoir est perdu {emoji_Sadge}",
            f"Le prochain GP c'est le bon {emoji_Copium}",
            f"Quoient... Encore un gp dead...",
            f"Mais nan c'est pas vrai ? C'est un gp dead ca ?? Ah bah j'suis ravi !!",
            f"Rentrons il commence à pleuvoir..."
        ]),
        get_random_string_from_array([
            f"It was at this moment that he knew... The gp was fucked up",
            f"EMOTIONAL DAMAGE",
            f"ALRIGHT EVERYBODY WAKE UP IT\'S NOT DEAD I PROMISE, RNJESUS TOLD ME {emoji_Copium}",
            f"Let\'s just forget about it..."
        ])
    )