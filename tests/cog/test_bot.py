"""
Test the bot's main functions.
"""
from cog.bot import EmojiResolver, cmd_from_content
from tests.conftest import Guild, Emoji


def test_emoji__init__():
    guild = Guild('myguild')
    guild.emojis = [Emoji('duck'), Emoji('car'), Emoji('sleep')]
    emo = EmojiResolver()
    assert not emo.emojis


def test_emoji__str__():
    guild = Guild('myguild')
    guild.emojis = [Emoji('car'), Emoji('duck'), Emoji('sleep')]
    emo = EmojiResolver()
    emo.update([guild])

    assert "'duck': Emoji: Emoji-5 duck," in str(emo)


def test_emoji_update():
    guild = Guild('myguild')
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
    guild = Guild('myguild')
    guild.emojis = [Emoji('car'), Emoji('duck'), Emoji('sleep')]
    emo = EmojiResolver()
    emo.update([guild])

    assert emo.fix(":duck: :sleep:", guild) == "[duck] [sleep]"


def test_cmd_from_content_no_space():
    content = "!fort"
    prefix = '!'

    assert "fort" == cmd_from_content(prefix, content)


def test_cmd_from_content_has_space():
    content = "!bgs subCmd some text"
    prefix = '!'

    assert "bgs" == cmd_from_content(prefix, content)
