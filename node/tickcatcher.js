const autobahn = require('autobahn');
const wsuri = "wss://api.poloniex.com";
const connection = new autobahn.Connection({
  url: wsuri,
  realm: "realm1"
});
 
connection.onopen = session => {
    console.log('Opening connection')
    function marketEvent (args,kwargs) {
        console.log('marketEvent',args);
    }
    function tickerEvent (args,kwargs) {
        console.log('tickerEvent',args);
    }
    function trollboxEvent (args,kwargs) {
        console.log('trollboxEvent',args);
    }
    session.subscribe('BTC_XMR', marketEvent);
    session.subscribe('ticker', tickerEvent);
    session.subscribe('trollbox', trollboxEvent);
}
 
connection.onclose = () => console.log('Websocket connection closed')

connection.open();