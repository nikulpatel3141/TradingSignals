
window.addEventListener("DOMContentLoaded", () => {
  const websocket = new WebSocket("ws://localhost:8001/");
  
  websocket.addEventListener("message", ({ data }) => {
    const event = JSON.parse(data);
    console.log(event);
    // do something with event
  });
  
});
