from pprint import pprint
import asyncio

from transparentepstein.ingestion import scraper, stages, selectors, services
from transparentepstein.core.db import close_apool
from transparentepstein.core.logging import setup_logging

async def async_main():
    try:
        setup_logging()
        # await stages.discover_stage()
        # await stages.fetch_stage()
        # await stages.move_to_load_stage()
        # await stages.load_stage()
        chunks = services.chunk_text(
            size=200,
            overlap=20,
            text="""
To: 
From: 
Sent: 
Tue 5/16/ 
• 
• 
M 
Subject: 
Ticket for 
May 21-29 
Title: American Express 
OK, non refundable coach , DL - $200 change fee, Jet Blue - $150.00. 
Regards, 
Natalia (Natasha) Molotkova 
Centurion Relationship Manager 
Natalia Molotkova 
Hours: Mon, Wed 9a-4p, 530p-7p 
Tue, Thur, Fri 9a - 530p EST 
Perfect! Please issue ticket! 
thanks 
On Ma 16 2017 at 4:01 PM Natalia Molotkova 
< 
wrote: 
Outbound flight: 
DL 
898 21MAY LGA PBI 1020A 0128P 
TOTAL FARE - USD 206.20 
Return 
B6 
62 29MAY PBI LGA 0958A 1248P 
TOTAL FARE - USD 278.80 
Regards, 
Natalia (Natasha) Molotkova 
Centurion Relationship Manager 
Hours: Mon, Wed 9a-4p, 530p-7p 
Tue, Thur, Fri 9a - 530p EST 
EFTA_R1_00936402 
EFTA02212929

I figured as much! thanks 
On Ma 16 2017 at 3:58 PM Natalia Molotkova 
> wrote: 
Thank you for reminding, it is still on my to do list, got so much stuff for travel sooner, on it right 
now. 
Regards, 
Natalia (Natasha) Molotkova 
Centurion Relationship Manager 
Hours: Mon, Wed 9a-4p, 530p-7p 
Tue, Thur, Fri 9a - 530p EST 
Hi Natasha...I think this may have slipped through...never rec'd any ticket info back from you! 
On Ma 16 2017 at 9:14 AM. Natalia Molotkova 
> wrote: 
Morning, on it. 
Regards, 
Natalia (Natasha) Molotkova 
Centurion Relationship Manager 
Hours: Mon, Wed 9a-4p, 530p-7p 
Tue, Thur, Fri 9a - 530p EST 
Morning! We need a round trip, coach ticket for 
to fly from NY to FL (preferably 
LGA) May 21 return May 29. Flight on 21st around 11am or so...flight on 29th around 
10am...Timing is flexible however...What is best price for these dates...? Thanks! 
EFTA_R1_00936403 
EFTA02212930

Pnvaoy Statement I ' . 
To loam more about e-mail security or report a suspicious e-mail, please visit us at amencanexoress comfohishing. 
O 2015 American Express. All rights reserved 
_ 
American Express uses 3rd party concierge service providers who are not authorized to act on behalf of American Express 
\ 
and you acknowledge that American Express is in no way responsible or liable for the actions of the service provider and the 
only remedy for any claims relating to services or products provided by the service provider is against the service provider and 
not against American Express. You are responsible for any purchases. shipping charges anclfor fees you authorize. We 
reserve the right to note profile and preference data for servicing purposes. 
EFTA_R1_00936404 
EFTA02212931
        """)
        
        print(chunks)
    finally:
        await close_apool()
    
# UV workaround: https://github.com/astral-sh/uv/issues/18931
def main() -> None:
    asyncio.run(async_main())