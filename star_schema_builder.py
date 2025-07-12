import pandas as pd 
import json

raw_df = pd.read_excel('raw_data.xlsx')

def clean_raw_content(raw_text):   #remove extra brackets
    if not isinstance(raw_text, str):   #check if input is a string
        return {}
 
    raw_text = raw_text.strip()   #trim any whitespace characters, tabs as well 

    #try to find correct JSON closing 
    max_len = len(raw_text)
    for i in range(max_len, 0, -1):   #backwards, chops off characters from the end  -> (start, stop, step) 
        try:
            firstChar = raw_text[:i]       #take first charactes, parse it as a json; [:i] removes characters from the end
            parsed = json.loads(firstChar)
            return parsed     #return parsed dictionary or list  
        except json.JSONDecodeError:
            continue

    return {}   # If nothing worked

raw_df['raw_parsed'] = raw_df['raw_content'].apply(clean_raw_content)    #function is applied to each row -> axis = 1
parsed_df = pd.json_normalize(raw_df['raw_parsed']).rename(columns = {'id': 'raw_id2'})      #from parsed dictionaries into columns  

main_df = pd.concat([raw_df.drop(columns = ['raw_content', 'raw_parsed']), parsed_df], axis = 1)    #drop raw_content and raw_parsed, combine 
                                                                                                    #original columns and new flat columns

print("Available columns after flattening: ")
print(main_df.columns.tolist())  

#dim_comm_type          selecting df with two [ [ ] ] as a df, if we use one [] it will be series
dim_comm_type = main_df[['comm_type']].drop_duplicates().reset_index(drop = True) #drop=True → avoids adding an extra index column
dim_comm_type['comm_type_id'] = dim_comm_type.index + 1

main_df = main_df.merge(dim_comm_type, on = 'comm_type', how = 'left')   #adds the comm_type_id column back into the main table (main_df)

#dim_subject
dim_subject = main_df[['subject']].drop_duplicates().reset_index(drop = True)  
dim_subject['subject_id'] = dim_subject.index + 1

main_df = main_df.merge(dim_subject, on = 'subject', how = 'left') #left means left-join, keep all data from the main_df table, and add 
                                                                   #matching from dim_subject

#dim_user
def extract_emails(row):    #collect all emails from all user-related fields
    emails = set()          #an empty set to store unique email addresses, avoids duplicate emails

    emails.add(row.get('host_email')) 
    emails.add(row.get('organizer_email'))

    emails.update(row.get('participants', []))  #list
    emails.update([s.get('email', '') for s in row.get('meeting_attendees', [])])  

    return list(filter(None, emails))     #Converts the set to a list, filter(None, emails) removes any empty strings "", None values

speaker_names_master = []
used_speakers = set()

def prep_speaker_list(speakers):
    return [
        {
            'full_name': s.get('name', '').strip(),
            'first': s.get('name', '').split()[0].lower() if s.get('name') else '',
            'last': s.get('name', '').split()[-1].lower() if s.get('name') else '',
        }
        for s in speakers if s.get('name')
    ]



def infer_name(email, speakers):
    if not email:
        return None

    username = email.split('@')[0].lower().replace('.', '').replace('_', '')

    candidates = []
    for speaker in speakers:
        full = speaker['full_name']
        first = speaker['first']
        last = speaker['last']
        score = 0

        if first in username and last in username:
            score = 2
        elif first in username or last in username:
            score = 1

        if score > 0 and full not in used_speakers:
            candidates.append((score, full))

    # Pick best match and mark it used
    if candidates:
        candidates.sort(reverse=True)  # highest score first
        selected = candidates[0][1]
        used_speakers.add(selected)
        return selected

    return None



user_details = {}

for _, row in raw_df.iterrows():
    try:
        parsed = json.loads(row['raw_parsed']) if isinstance(row['raw_parsed'], str) else row['raw_parsed']

        #=============================
        speakers = prep_speaker_list(parsed.get('speakers', []))

        #speakers = parsed.get('speakers', [])
        attendees = parsed.get('meeting_attendees', [])
        emails = extract_emails(parsed)

        for attendee in attendees:
            email = attendee.get('email')
            if not email:
                continue

            if email not in user_details:
                user_details[email] = {
                    'name': attendee.get('name') or infer_name(email, speakers),
                    'location': attendee.get('location'),
                    'displayName': attendee.get('displayName'),
                    'phoneNumber': attendee.get('phoneNumber'),
                }

        for email in emails:
            if email not in user_details:
                # Try to infer name for speakers or participants
                user_details[email] = {
                    'name': infer_name(email, speakers),
                    'location': None,
                    'displayName': None,
                    'phoneNumber': None,
                }
    except:
        continue
 
user_email_records = [] 
for _, row in raw_df.iterrows():
    try:
        content = json.loads(row['raw_parsed']) if isinstance(row['raw_parsed'], str) else row['raw_parsed'] #checks the type, if it's a string,
        user_emails = extract_emails(content)                                                                # it parses it with json.loads()

        for email in user_emails:
            user_email_records.append(email)    
    except:
        continue

#Deduplicate        `                       `
dim_user = pd.DataFrame([
    {'email': email, **details} for email, details in user_details.items() if email
])
dim_user['user_id'] = range(1, len(dim_user) + 1)   
  
#dim_calendar
dim_calendar = main_df[['calendar_id']].dropna().drop_duplicates().reset_index(drop = True) # result will be a DataFrame instead of a Series 
dim_calendar['calendar_id_surrogate'] = dim_calendar.index + 1
main_df = main_df.merge(dim_calendar, on = 'calendar_id', how = 'left') 

#dim_audio
dim_audio = main_df[['audio_url']].dropna().drop_duplicates().reset_index(drop = True)
dim_audio['audio_id'] = dim_audio.index + 1
main_df = main_df.merge(dim_audio, on = 'audio_url', how = 'left')

#dim_video
dim_video = main_df[['video_url']].dropna().drop_duplicates().reset_index(drop = True)
dim_video['video_id'] = dim_video.index + 1
main_df = main_df.merge(dim_video, on = 'video_url', how = 'left')

#dim_transcript
dim_transcript = main_df[['transcript_url']].dropna().drop_duplicates().reset_index(drop = True)
dim_transcript['transcript_id'] = dim_transcript.index + 1
main_df = main_df.merge(dim_transcript, on = 'transcript_url', how = 'left')

#Fact table
fact_df = main_df[[
    'id', 'raw_id2', 'comm_type_id', 'subject_id', 'calendar_id_surrogate', 'audio_id', 'video_id', 'transcript_id', 'title', 'duration', 
    'host_email', 'organizer_email', 'ingested_at', 'processed_at', 'is_processed'
]].copy()  

fact_df = fact_df.rename(columns = {'id': 'communication_id'})  

fact_df = fact_df.merge(dim_user.rename(columns = {'email': 'host_email', 'user_id': 'host_id'}), on = 'host_email', how = 'left')
fact_df = fact_df.merge(dim_user.rename(columns = {'email': 'organizer_email', 'user_id': 'organizer_id'}), on = 'organizer_email', how = 'left') 

fact_df = fact_df.drop(columns=[col for col in fact_df.columns if col.endswith('_x') or col.endswith('_y')])  

fact_df = fact_df.loc[:, ~fact_df.columns.duplicated()]   #remove duplicates;            : means all rows, ~ -> bitwise NOT --- 
                                                          #df.loc[rows, columns]         selects only non-duplicate columns

#bridge table n 
bridge_rows = []

for _, row in raw_df.iterrows():
    try:
        meeting_id = row['id']
        parsed = json.loads(row['raw_parsed']) if isinstance(row['raw_parsed'], str) else row['raw_parsed']

        meeting_speakers = prep_speaker_list(parsed.get('speakers', []))  # fresh speaker dicts
        speaker_names = set(s['full_name'] for s in meeting_speakers)

        attendee_emails = [att.get('email') for att in parsed.get('meeting_attendees', []) if att.get('email')]
        participant_emails = parsed.get('participants', [])
        host_email = parsed.get('host_email')
        organizer_email = parsed.get('organizer_email')

        all_emails = set(attendee_emails + participant_emails + [host_email, organizer_email])

        speaker_emails = set()
        for email in all_emails:
            name = user_details.get(email, {}).get('name')
            if name and name in speaker_names:
                speaker_emails.add(email)


        for email in all_emails:
            user_id = dim_user[dim_user['email'] == email]['user_id'].values[0]

            bridge_rows.append({
                'communication_id': meeting_id,
                'user_id': user_id,
                'isAttendee': email in attendee_emails,
                'isParticipant': email in participant_emails,
                'isSpeaker': email in speaker_emails,
                'isOrganizer': email == organizer_email
            })
    except:
        continue

bridge_comm_user = pd.DataFrame(bridge_rows).drop_duplicates()   #remove duplicates

#Export to excel
with pd.ExcelWriter('star_schema_output.xlsx', engine = 'xlsxwriter') as writer:
    dim_comm_type.to_excel(writer, sheet_name = 'dim_comm_type', index = False)
    dim_subject.to_excel(writer, sheet_name = 'dim_subject', index = False)
    dim_user.to_excel(writer, sheet_name = 'dim_user', index = False)   #Do not write the DataFrame's index as a separate column in excel 
    dim_calendar.to_excel(writer, sheet_name = 'dim_calendar', index = False)
    dim_audio.to_excel(writer, sheet_name = 'dim_audio', index = False)
    dim_video.to_excel(writer, sheet_name = 'dim_video', index = False)
    dim_transcript.to_excel(writer, sheet_name = 'dim_transcript', index = False)
    fact_df.to_excel(writer, sheet_name = 'fast_communication', index = False)
    bridge_comm_user.to_excel(writer, sheet_name = 'bridge_comm_user', index = False)

print("Star schema exported to star_schema_output.xlsx")  

#print(main_df['video_url'].value_counts(dropna=False))   #number of rows woth None, NaN, empty strings