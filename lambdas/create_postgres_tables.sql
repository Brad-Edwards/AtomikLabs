-- Create Podcasts Table
CREATE TABLE Podcasts (
    podcast_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    url VARCHAR(512),
    hosting_platform VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Podcast Episodes Table
CREATE TABLE PodcastEpisodes (
    episode_id SERIAL PRIMARY KEY,
    podcast_id INT REFERENCES Podcasts(podcast_id),
    date DATE NOT NULL,
    published_date DATE,
    status VARCHAR(50) NOT NULL,
    script TEXT,
    show_notes TEXT,
    themes_text TEXT,
    social_post_text TEXT,
    episode_url VARCHAR(512),
    audio_file_url VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Newsletters Table
CREATE TABLE Newsletters (
    newsletter_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    url VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Newsletter Editions Table
CREATE TABLE NewsletterEditions (
    edition_id SERIAL PRIMARY KEY,
    newsletter_id INT REFERENCES Newsletters(newsletter_id),
    date DATE NOT NULL,
    published_date DATE,
    status VARCHAR(50) NOT NULL,
    content TEXT,
    edition_url VARCHAR(512),
    tied_podcast_id INT REFERENCES Podcasts(podcast_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Arxiv Summaries Table
CREATE TABLE ArxivSummaries (
    summary_id SERIAL PRIMARY KEY,
    unique_identifier VARCHAR(255) NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create PodcastEpisode_Summary Junction Table
CREATE TABLE PodcastEpisode_Summary (
    episode_id INT REFERENCES PodcastEpisodes(episode_id),
    summary_id INT REFERENCES ArxivSummaries(summary_id),
    PRIMARY KEY (episode_id, summary_id)
);

-- Create NewsletterEdition_Summary Junction Table
CREATE TABLE NewsletterEdition_Summary (
    edition_id INT REFERENCES NewsletterEditions(edition_id),
    summary_id INT REFERENCES ArxivSummaries(summary_id),
    PRIMARY KEY (edition_id, summary_id)
);

-- Create SocialShares Table
CREATE TABLE SocialShares (
    share_id SERIAL PRIMARY KEY,
    episode_id INT REFERENCES PodcastEpisodes(episode_id),
    edition_id INT REFERENCES NewsletterEditions(edition_id),
    platform VARCHAR(255),
    share_url VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
