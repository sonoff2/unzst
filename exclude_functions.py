def exclude_bot_authors(author):
    return 'bot' not in str(author).lower()

def exclude_deleted(txt):
    return '[deleted]' not in txt and '[removed]' not in txt