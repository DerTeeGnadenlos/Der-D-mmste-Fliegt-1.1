const http=require('http');
const path=require('path');
const crypto=require('crypto');
const express=require('express');
const WebSocket=require('ws');

const app=express();
const server=http.createServer(app);
const wss=new WebSocket.Server({server});
const rooms=new Map();

app.use((req,res,next)=>{res.setHeader('Access-Control-Allow-Origin','*');res.setHeader('Access-Control-Allow-Headers','Content-Type');next();});
app.use(express.static(path.join(__dirname,'public')));
app.get('/health',(_,res)=>res.json({ok:true,version:'2.0.16-signal-routing-fix'}));
app.get('/api/ice',(_,res)=>{
  const iceServers=[
    {urls:'stun:stun.relay.metered.ca:80'}
  ];

  if(process.env.TURN_USERNAME&&process.env.TURN_CREDENTIAL){
    const username=process.env.TURN_USERNAME;
    const credential=process.env.TURN_CREDENTIAL;
    const configured=String(process.env.TURN_URL||'').trim();

    if(configured){
      // Den exakt getesteten TURN-Endpunkt unverändert zuerst verwenden.
      iceServers.push({urls:configured,username,credential});

      // Falls Metered verwendet wird, zusätzliche Fallbacks ergänzen.
      if(configured.includes('standard.relay.metered.ca')){
        const fallbacks=[
          'turn:standard.relay.metered.ca:80?transport=tcp',
          'turn:standard.relay.metered.ca:443',
          'turns:standard.relay.metered.ca:443?transport=tcp'
        ];
        for(const url of fallbacks){
          if(url!==configured)iceServers.push({urls:url,username,credential});
        }
      }
    }
  }

  res.json({iceServers,forceRelay:false});
});
app.get('/join/:room',(_,res)=>res.sendFile(path.join(__dirname,'public','player.html')));

function send(ws,msg){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));}
function roomCode(){const a='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';let s='';for(let i=0;i<6;i++)s+=a[crypto.randomInt(a.length)];return s;}
function publicPlayer(p){return {id:p.id,name:p.name,lives:p.lives,safe:p.safe,connected:!!p.ws&&p.ws.readyState===WebSocket.OPEN,sessionToken:p.sessionToken};}
function publicPlayers(room){return [...room.players.values()].map(publicPlayer);}
function broadcastPlayers(room,msg){for(const p of room.players.values())send(p.ws,msg);}
function candidatesForVote(room){return [...room.players.values()].filter(p=>p.lives>0&&!p.safe).map(p=>({id:p.id,name:p.name}));}


function sendToPlayer(roomCode,playerId,payload){
 const r=rooms.get(roomCode);
 if(!r)return false;
 const p=r.players?.get ? r.players.get(playerId) : null;
 const sock=p?.ws;
 if(sock&&sock.readyState===1){
   sock.send(JSON.stringify(payload));
   return true;
 }
 // Fallback: search sockets carrying matching room/player metadata.
 for(const client of wss.clients){
   if(client.readyState===1 && client.room===roomCode && client.playerId===playerId){
     client.send(JSON.stringify(payload));
     return true;
   }
 }
 return false;
}

wss.on('connection',ws=>{
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw.toString())}catch{return;}

    if(m.type==='create-room'){
      let code=roomCode();while(rooms.has(code))code=roomCode();
      const room={code,host:ws,players:new Map(),vote:null,tiebreak:null};
      rooms.set(code,room);Object.assign(ws,{role:'host',room:code,id:'host'});
      send(ws,{type:'room-created',room:code,players:[]});return;
    }

    const code=String(m.room||ws.room||'').toUpperCase();
    const room=rooms.get(code);
    if(!room){send(ws,{type:'error',message:'Raum nicht gefunden.'});return;}

    if(m.type==='join-room'){
      let player=null;
      if(m.sessionToken){player=[...room.players.values()].find(p=>p.sessionToken===m.sessionToken)||null;}
      if(player){
        player.ws=ws;player.name=String(m.name||player.name).trim().slice(0,30)||player.name;
        Object.assign(ws,{role:'player',room:code,id:player.id,sessionToken:player.sessionToken});
        send(ws,{type:'joined',room:code,player:publicPlayer(player),reconnected:true});
        send(room.host,{type:'player-reconnected',player:publicPlayer(player)});
      }else{
        const id=crypto.randomUUID(),sessionToken=crypto.randomBytes(18).toString('hex');
        player={id,sessionToken,name:String(m.name||'Spieler').trim().slice(0,30)||'Spieler',lives:3,safe:false,ws};
        room.players.set(id,player);Object.assign(ws,{role:'player',room:code,id,sessionToken});
        send(ws,{type:'joined',room:code,player:publicPlayer(player),reconnected:false});
        send(room.host,{type:'player-joined',player:publicPlayer(player)});
      }
      if(room.vote?.active){send(ws,{type:'vote-start',voteId:room.vote.id,candidates:room.vote.candidates});}
      if(room.tiebreak?.active&&room.tiebreak.participants.includes(ws.id)){send(ws,{type:'tiebreak-start',tiebreakId:room.tiebreak.id});}
      return;
    }

    if(ws.role==='player'){
      if(['offer','answer','ice'].includes(m.type)){send(room.host,{...m,playerId:ws.id});return;}
      if(m.type==='submit-vote'&&room.vote?.active&&m.voteId===room.vote.id){
        const voter=room.players.get(ws.id);if(!voter||voter.lives<=0)return;
        const valid=room.vote.candidates.some(c=>c.id===m.targetId);if(!valid)return;
        room.vote.votes.set(ws.id,m.targetId);
        send(ws,{type:'vote-accepted',targetId:m.targetId});
        send(room.host,{type:'vote-progress',voteId:room.vote.id,submitted:room.vote.votes.size,total:[...room.players.values()].filter(p=>p.lives>0).length});
        return;
      }
      if(m.type==='submit-tiebreak'&&room.tiebreak?.active&&m.tiebreakId===room.tiebreak.id&&room.tiebreak.participants.includes(ws.id)){
        const value=String(m.value??'').trim().slice(0,30);if(!value)return;
        room.tiebreak.values.set(ws.id,value);
        send(ws,{type:'tiebreak-accepted'});
        send(room.host,{type:'tiebreak-progress',submitted:room.tiebreak.values.size,total:room.tiebreak.participants.length});
        return;
      }
      return;
    }

    if(ws.role==='host'){
      if(['offer','answer','ice'].includes(m.type)){
        const p=room.players.get(m.playerId);if(p)send(p.ws,{...m,from:'host'});return;
      }
      if(m.type==='state'){
        const p=room.players.get(m.playerId);if(!p)return;
        if(typeof m.name==='string')p.name=m.name.trim().slice(0,30)||p.name;
        if(Number.isFinite(m.lives))p.lives=Math.max(0,Math.min(99,m.lives));
        if(typeof m.safe==='boolean')p.safe=m.safe;
        send(p.ws,{type:'state',name:p.name,lives:p.lives,safe:p.safe});return;
      }
      if(m.type==='kick'){
        const p=room.players.get(m.playerId);if(p){send(p.ws,{type:'kicked'});p.ws?.close();room.players.delete(p.id);send(ws,{type:'player-removed',playerId:p.id});}return;
      }
      if(m.type==='broadcast-effect'){broadcastPlayers(room,{type:'effect',effect:m.effect,volume:typeof m.volume==='number'?m.volume:1});return;}
      if(m.type==='start-vote'){
        const candidates=candidatesForVote(room);
        const vote={id:crypto.randomUUID(),active:true,candidates,votes:new Map()};room.vote=vote;
        broadcastPlayers(room,{type:'vote-start',voteId:vote.id,candidates});
        send(ws,{type:'vote-started',voteId:vote.id,total:[...room.players.values()].filter(p=>p.lives>0).length});return;
      }
      if(m.type==='end-vote'&&room.vote?.active){
        room.vote.active=false;
        const counts={};for(const c of room.vote.candidates)counts[c.id]=0;
        const reveal=[];
        for(const [voterId,targetId] of room.vote.votes){counts[targetId]=(counts[targetId]||0)+1;reveal.push({voterId,targetId});}
        const max=Math.max(0,...Object.values(counts));
        const leaders=Object.keys(counts).filter(id=>counts[id]===max&&max>0);
        broadcastPlayers(room,{type:'vote-ended'});
        send(ws,{type:'vote-result',counts,reveal,leaders,max});return;
      }
      if(m.type==='start-tiebreak'){
        const ids=(m.playerIds||[]).filter(id=>room.players.has(id));if(ids.length<2)return;
        const tb={id:crypto.randomUUID(),active:true,participants:ids,values:new Map()};room.tiebreak=tb;
        for(const id of ids)send(room.players.get(id).ws,{type:'tiebreak-start',tiebreakId:tb.id});
        send(ws,{type:'tiebreak-started',tiebreakId:tb.id,total:ids.length});return;
      }
      if(m.type==='reveal-tiebreak'&&room.tiebreak?.active){
        room.tiebreak.active=false;const values=[];
        for(const id of room.tiebreak.participants)values.push({playerId:id,value:room.tiebreak.values.get(id)??'—'});
        for(const id of room.tiebreak.participants)send(room.players.get(id).ws,{type:'tiebreak-ended'});
        send(ws,{type:'tiebreak-result',values});return;
      }
      if(m.type==='set-player-volume'){
        const p=room.players.get(m.playerId);if(p)send(p.ws,{type:'host-volume',value:m.value});return;
      }
    }
  });

  ws.on('close',()=>{
    const room=rooms.get(ws.room);if(!room)return;
    if(ws.role==='host'){
      for(const p of room.players.values())send(p.ws,{type:'host-left'});
      rooms.delete(room.code);
    }else if(ws.role==='player'){
      const p=room.players.get(ws.id);if(p){p.ws=null;send(room.host,{type:'player-disconnected',player:publicPlayer(p)});}
    }
  });
});

const port=process.env.PORT||3000;
server.listen(port,'0.0.0.0',()=>console.log('Der Dümmste fliegt v2 Server auf Port '+port));
