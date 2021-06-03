"""
Test the bot's main functions.
"""
from cog.bot import EmojiResolver
from tests.conftest import Server, Emoji


def test_emoji__init__():
    guild = Server('myguild')
    guild.emojis = [Emoji('duck'), Emoji('car'), Emoji('sleep')]
    emo = EmojiResolver()
    assert emo.emojis == {}


def test_emoji__str__():
    guild = Server('myguild')
    guild.emojis = [Emoji('car'), Emoji('duck'), Emoji('sleep')]
    emo = EmojiResolver()
    emo.update([guild])

    assert "'duck': Emoji: Emoji-5 duck," in str(emo)


def test_emoji_update():
    guild = Server('myguild')
    guild.emojis = [Emoji('car'), Emoji('duck'), Emoji('sleep')]
    emo = EmojiResolver()
    emo.update([guild])

    expected = {
        guild.id: {
            'car': guild.emojis[0],
            'duck': guild.emojis[1],
            'sleep': guild.emojis[2],
        }
    }
    assert emo.emojis == expected


def test_emoji_fix():
    guild = Server('myguild')
    guild.emojis = [Emoji('car'), Emoji('duck'), Emoji('sleep')]
    emo = EmojiResolver()
    emo.update([guild])

    assert emo.fix(":duck: :sleep:", guild) == "[duck] [sleep]"
