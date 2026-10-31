# Recording guide: what to record on TikTok, step by step (checkpoint C1)

These recordings show what logged-out TikTok actually sends a browser: the data in each page, the calls the web app makes, the cookies, and where it stops. They settle several open questions ([open-questions.md](open-questions.md)) and give the probe its exact request shapes. The same kind of recordings settled most of igs's questions ([igs HAR findings](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/instagram/har-findings-2026-10-30.md)).

**Time needed:** about 30 minutes for the six main recordings.

**What you need:** Chrome on your Mac, **not logged in to TikTok**, and 2 accounts from your list:
- **account A:** a busy one (a TV channel or news outlet that posts several videos a day and gets many comments);
- **account B:** a small one with fewer than 60 videos, so its grid can be scrolled to the end.

**The files are private.** They hold other people's names and comments. Keep them in your Drive folder only, and share the folder by link with the build session. The session saves them in a git-ignored folder and never commits them.

---

## Before you start (once)

1. Open Chrome and press **Cmd-Shift-N** to open a new **Incognito** window. Use this window for every recording below. If TikTok asks you to log in, close the prompt; never log in.
2. Press **Cmd-Option-I** to open DevTools, and click the **Network** tab.
3. In the Network tab, tick **Preserve log** and **Disable cache**.
4. Click the gear icon (top right of DevTools, "Settings"). Under **Preferences**, find **Network** and tick **Allow to generate HAR with sensitive data**. Close Settings.
   - Why: without it, Chrome removes the cookies from the recording, and TikTok's cookies are one of the open questions (T2). You are logged out, so they carry no account or password.
5. Write down, for the notes you send at the end: your country, whether you are on a home or office connection, and Chrome's version (menu **Chrome → About Google Chrome**).

**To save each recording** (used at the end of every step below):
1. Right-click any row in the Network list.
2. Choose **Save all as HAR (with sensitive data)**. If your Chrome shows only **Save all as HAR with content**, choose that.
3. Save it under the name the step gives (for example `R1-profile-A.har`).
4. Click the **clear** button (the circle with a line through it, top left of the Network tab) before the next step, so each file holds one step.

---

## R1: account A's profile page, scrolled

1. In the Incognito window's address bar, open `https://www.tiktok.com/@ACCOUNT_A` (account A's username).
2. Wait until the videos appear. If a cookie banner appears, click **Decline optional cookies** (or the closest choice).
3. Scroll down the video grid **slowly**, a screen at a time, pausing 2 to 3 seconds each time. Keep going for about 2 minutes, or until no more videos load, or a login or "Verify to continue" box blocks you.
4. Note: about how many rows of videos loaded, the date of the oldest video you can see (hover a video or open its link to see the date if needed), whether any video shows a **Pinned** label, and anything that blocked you.
5. Save as `R1-profile-A.har`, then clear.

## R2: a video with many comments, opened from the grid

1. Still on account A's page, scroll back up and **click** a video that shows many comments (hundreds if possible). It opens in the player with the comments beside it.
2. Wait 5 seconds. Scroll the **comments** down slowly for about a minute, so several more batches load.
3. On 2 or 3 comments that say **View replies** (or "View N replies"), click it. If more replies load with **View more**, click that once too.
4. Note: the comment count the video shows, and whether you saw any "Log in to see comments" message.
5. Press **Esc** to close the video.
6. Save as `R2-video-comments-A.har`, then clear.

## R3: a video page opened directly

1. Copy the link of a different video of account A (right-click the video in the grid → **Copy link address**).
2. In the **same tab**, click the address bar, paste the link and press Enter. Typing the link loads the whole page fresh, which is what this step records.
3. Let the video play for 10 seconds, then scroll its comments a little.
4. Save as `R3-video-direct-A.har`, then clear.

## R4: a photo post

1. Find a **photo post** on any account in your list: photo posts show an image icon on the grid tile, and their link contains `/photo/` instead of `/video/`.
2. Open its link directly in the same tab (paste into the address bar and press Enter).
3. Click through 2 or 3 of its photos with the arrows.
4. Save as `R4-photo.har`, then clear. (If you cannot find one in 5 minutes, skip this step and say so in your notes.)

## R5: account B, scrolled to the end

1. Open `https://www.tiktok.com/@ACCOUNT_B`.
2. Scroll the grid slowly to the **very end**, until no more videos load.
3. Note how many videos the profile says it has, and how many you saw.
4. Save as `R5-profile-B-end.har`, then clear.

## R6: a sponsored or "Paid partnership" video

1. Look on brand accounts in your list for a video labelled **Paid partnership**, **Sponsored**, **Ad** or **Promotional content**. Open it directly by its link.
2. Save as `R6-labelled.har`, then clear. (If you find none in 5 minutes, skip this step and say so.)

## R7 (optional, 2 minutes): the embed and oEmbed views

1. Take the link of any video of account A. In the address bar open `https://www.tiktok.com/oembed?url=` followed by the video link, and press Enter.
2. Then open `https://www.tiktok.com/embed/v2/VIDEO_ID`, where `VIDEO_ID` is the long number at the end of the video link.
3. Save both in one file as `R7-embed.har`, then clear.

---

## Send them to the build session

1. Put the `.har` files in your Drive folder (zip any file over 500 MB first).
2. Write a short note with what you wrote down: country and connection, Chrome version, accounts A and B (their usernames go in the note, not in any repository), and what you saw in R1 to R6 (rows loaded, oldest date, pinned labels, blocks, comment counts, replies, the end of B's grid).
3. Share the folder **by link** and paste the link and your note into the build session.

The session downloads the files into `.local/har/` (git-ignored), analyses them, and writes `docs/tiktok/har-findings-<date>.md` with request shapes, field names and counts only: no usernames, names or comment text.
