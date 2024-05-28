# Ticketmaster API Crawler

This project is a crawler for the Ticketmaster API. It allows you to retrieve event data from Ticketmaster and perform various operations on the data.

## Get the API Key

1. Go to the [Ticketmaster Developer Portal](https://developer.ticketmaster.com/)
2. Sign up for an account
3. Create a new app
4. Get the API key

## Installation

1. Clone the repository: `git clone https://github.com/your-username/ticketmaster-api-crawler.git`
2. Install virtualenv: `pip install virtualenv`
3. Create a virtual environment: `virtualenv venv`
4. Activate the virtual environment: `source venv/bin/activate`
5. Install the required dependencies: `pip install -r requirements.txt`

## Usage

1. Set up your Ticketmaster API credentials by creating a `.env` file and adding the following variables:
   ```
   API_KEY=your-api-key
   ```
2. Run the crawler: `python main.py`

## Testing

To run the tests, use the following command:

```bash
pytest tests
```

## Features

- Retrieve event data from the Ticketmaster API
- Filter events based on location, date, and other criteria
- Store event data in a database for further analysis

## Future Work

- Add more filters and search criteria
- Write more tests
- Get more data from the API
- Deploy the crawler to a AWS Lambda functions

## Contributing

Contributions are welcome! If you have any ideas or improvements, feel free to open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
