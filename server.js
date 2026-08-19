const http=require("http");
const path=require("path");
const crypto=require("crypto");
const express=require("express");
const WebSocket=require("ws");
const app=express();
const server=http.createServer(app);
const wss=new WebSocket.Server({server});
const rooms=new Map();

app.use((req,res,next)=>{
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  next();
});
app.use(express.static(path.join(__dirname,"public")));
app.get("/health",(_,res)=>res.json({ok:true}));
app.get("/api/ice",(_,res)=>{
  const iceServers=[{urls:["stun:stun.l.google.com:19302","stun:stun1.l.google.com:19302"]}];
  if(process.env.TURN_URL&&process.env.TURN_USERNAME&&process.env.TURN_CREDENTIAL){
    iceServers.push({
      urls:process.env.TURN_URL.split(",").map(x=>x.trim()).filter(Boolean),
      username:process.env.TURN_USERNAME,
      credential:process.env.TURN_CREDENTIAL
    });
  }
  res.json({iceServers});
});
app.get("/join/:room",(_,res)=>res.sendFile(path.join(__dirname,"public","player.html")));

function send(ws,msg){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg))}
function code(){
  const a="ABCDEFGHJKLMNPQRSTUVWXYZ23456789";let s="";
  for(let i=0;i<6;i++)s+=a[crypto.randomInt(a.length)];
  return s;
}

wss.on("connection",ws=>{
  ws.on("message",raw=>{
    let m;try{m=JSON.parse(raw.toString())}catch{return}
    if(m.type==="create-room"){
      let c=code();while(rooms.has(c))c=code();
      const room={code:c,host:ws,players:new Map()};
      rooms.set(c,room);Object.assign(ws,{role:"host",room:c,id:"host"});
      send(ws,{type:"room-created",room:c});return;
    }
    const c=String(m.room||ws.room||"").toUpperCase(),room=rooms.get(c);
    if(!room){send(ws,{type:"error",message:"Raum nicht gefunden."});return}
    if(m.type==="join-room"){
      if(!room.host||room.host.readyState!==WebSocket.OPEN){send(ws,{type:"error",message:"Host nicht verbunden."});return}
      const id=crypto.randomUUID();
      const p={id,name:String(m.name||"Spieler").trim().slice(0,30)||"Spieler",lives:3,safe:false,ws};
      Object.assign(ws,{role:"player",room:c,id});room.players.set(id,p);
      send(ws,{type:"joined",room:c,player:{id:p.id,name:p.name,lives:p.lives,safe:p.safe}});
      send(room.host,{type:"player-joined",player:{id:p.id,name:p.name,lives:p.lives,safe:p.safe}});
      return;
    }
    if(ws.role==="player"){
      if(["offer","answer","ice"].includes(m.type))send(room.host,{...m,playerId:ws.id});
      return;
    }
    if(ws.role==="host"){
      if(["offer","answer","ice"].includes(m.type)){
        const p=room.players.get(m.playerId);if(p)send(p.ws,{...m,from:"host"});
      }else if(m.type==="state"){
        const p=room.players.get(m.playerId);if(!p)return;
        if(typeof m.name==="string")p.name=m.name.trim().slice(0,30)||p.name;
        if(Number.isFinite(m.lives))p.lives=Math.max(0,Math.min(99,m.lives));
        if(typeof m.safe==="boolean")p.safe=m.safe;
        send(p.ws,{type:"state",name:p.name,lives:p.lives,safe:p.safe});
      }else if(m.type==="kick"){
        const p=room.players.get(m.playerId);if(p){send(p.ws,{type:"kicked"});p.ws.close()}
      }
    }
  });
  ws.on("close",()=>{
    const room=rooms.get(ws.room);if(!room)return;
    if(ws.role==="host"){
      for(const p of room.players.values())send(p.ws,{type:"host-left"});
      rooms.delete(room.code);
    }else{
      room.players.delete(ws.id);send(room.host,{type:"player-left",playerId:ws.id});
    }
  });
});
const port=process.env.PORT||3000;
server.listen(port,"0.0.0.0",()=>console.log("Server läuft auf Port "+port));