"""
Products Collection
{
  "_id": ObjectId("5f4e9b6b9a1d3b23d4f9e9a6"),
  "GroupID": ObjectId("5f4e9b6b9a1d3b23d4f9e9a4"),
  "Title": "Smartphone XYZ",
  "Description": "A top-notch smartphone with great features.",
  "Price": 499.99,
  "PostDate": ISODate("2021-12-10T12:00:00Z"),
  "Status": "Available",
  "ProductImageURLs": ["https://example.com/product_image.jpg"]
}


ProductCategories Collection (Many-to-Many Relationship)
{
  "_id": ObjectId("unique_link_id"),
  "product": ObjectId("product_id"),
  "category": ObjectId("category_id")
}


TelegramGroupChannel Collection
{
  "_id" : ObjectId(),   "Name": "Name of the group/channel",
  "GroupLink" : "Link to access the group/channel",
  "AdminContact" : "Contact information of the group admin",
  "VerifiedStatus ": "Whether the group/channel is verified",
  "Rating" : "Average rating of the group/channel",
  "ListingsCount" : "Number of products listed in the group",
  "MembersCount" : "Number of members in the group/channel",
  "GroupPicture" : "Profile picture of the group/channel",
  "Bio" : "Description or bio of the group/channel",
  "PostsPerDay" : "Average number of posts per day in the group",
  "LastMessage" : "Date of the last message in the group"
}

"""
from storage.store import fetch_stored_messages, store_raw_data


def preprocess():
    stored_messages = fetch_stored_messages(collection_name="raw_data")
    preprocessed = []
    for raw_message in stored_messages:
        filtered_data = extract_message_data(raw_message)
        if filtered_data:
            preprocessed.append(filtered_data)
    
    print('Storing preprocessed data...')
    stored = store_raw_data(preprocessed, collection_name="stage1_preprocessed_data")
    print('Success!' if stored else 'Fail!')



def extract_message_data(message_obj):
    """
    Extracts specific fields from a message object into a simplified dictionary.
    
    Parameters:
        message_obj (dict): The raw message object.

    Returns:
        dict: A dictionary containing the extracted fields.
    """
    try:
        # Get raw ID and message ID
        raw_id = message_obj.get('_id')
        message_id = message_obj.get('id')
        
        # Get channel ID
        peer_id = message_obj.get('peer_id', {})
        channel_id = peer_id.get('channel_id') if peer_id.get('_') == 'PeerChannel' else None

        # Extract additional fields
        date = message_obj.get('date')
        message = message_obj.get('message', '')
        forwards = message_obj.get('forwards', 0)
        views = message_obj.get('views', 0)

        if not message:
            return None

        # Extract reactions
        reactions_data = []
        reactions = message_obj.get('reactions', {})
        if reactions:
            results = reactions.get('results', [])
            for reaction in results:
                emoji = reaction.get('reaction', {}).get('emoticon', '')
                count = reaction.get('count', 0)
                if emoji:
                    reactions_data.append((emoji, count))

        return {
            'raw_id': raw_id,
            'message_id': message_id,
            'channel_id': channel_id,
            'date': date,
            'message': message,
            'forwards': forwards,
            'views': views,
            'reactions': reactions_data,
        }
    except:
        return None

