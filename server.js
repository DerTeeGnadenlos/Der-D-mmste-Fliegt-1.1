const http=require('http');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const express=require('express');
const WebSocket=require('ws');

const app=express();
app.use(express.json({limit:'8mb'}));
const mediaDir=path.join(require('os').tmpdir(),'ddf-media');
try{fs.mkdirSync(mediaDir,{recursive:true})}catch{}
const server=http.createServer(app);
const wss=new WebSocket.Server({server});
const rooms=new Map();

app.use((req,res,next)=>{res.setHeader('Access-Control-Allow-Origin','*');res.setHeader('Access-Control-Allow-Headers','Content-Type');next();});
app.use(express.static(path.join(__dirname,'public')));
app.use('/media',express.static(mediaDir,{maxAge:'1h'}));
app.post('/api/media',(req,res)=>{
  try{
    const data=String(req.body?.dataUrl||'');
    const m=data.match(/^data:image\/(png|jpeg|jpg|webp);base64,(.+)$/);
    if(!m)return res.status(400).json({ok:false,error:'Nur PNG/JPG/WEBP erlaubt.'});
    const buf=Buffer.from(m[2],'base64');
    if(buf.length>5*1024*1024)return res.status(413).json({ok:false,error:'Bild größer als 5 MB.'});
    const ext=m[1]==='jpeg'?'jpg':m[1];
    const id=crypto.randomUUID()+'.'+ext;
    fs.writeFileSync(path.join(mediaDir,id),buf);
    res.json({ok:true,url:'/media/'+id,size:buf.length});
  }catch(e){res.status(500).json({ok:false,error:e.message})}
});
app.get('/health',(_,res)=>res.json({ok:true,version:'3.0.0-gameshow-platform'}));
app.get('/api/ice',(_,res)=>{
  // Bewusst schlanke ICE-Liste:
  // 1 stabiler STUN-Dienst für direkte P2P-Verbindungen
  // + exakt der in Metered/Trickle-ICE erfolgreich getestete TURN-Endpunkt.
  const iceServers=[
    {urls:'stun:stun.l.google.com:19302'}
  ];

  if(process.env.TURN_USERNAME&&process.env.TURN_CREDENTIAL){
    const configured=String(process.env.TURN_URL||'').trim();
    if(configured){
      iceServers.push({
        urls:configured,
        username:process.env.TURN_USERNAME,
        credential:process.env.TURN_CREDENTIAL
      });
    }
  }

  res.json({iceServers,forceRelay:false});
});
app.get('/join/:room',(_,res)=>res.sendFile(path.join(__dirname,'public','player.html')));

function send(ws,msg){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));}
function roomCode(){const a='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';let s='';for(let i=0;i<6;i++)s+=a[crypto.randomInt(a.length)];return s;}
function publicPlayer(p){return {id:p.id,name:p.name,lives:p.lives,safe:p.safe,ready:!!p.ready,color:p.color||'#4e7cff',connected:!!p.ws&&p.ws.readyState===WebSocket.OPEN,sessionToken:p.sessionToken};}
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
      const room={code,host:ws,players:new Map(),vote:null,tiebreak:null,estimate:null,buzzer:null,imageQuestion:null,scene:'lobby'};
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
        player={id,sessionToken,name:String(m.name||'Spieler').trim().slice(0,30)||'Spieler',lives:3,safe:false,ready:false,color:'#4e7cff',ws};
        room.players.set(id,player);Object.assign(ws,{role:'player',room:code,id,sessionToken});
        send(ws,{type:'joined',room:code,player:publicPlayer(player),reconnected:false});
        send(room.host,{type:'player-joined',player:publicPlayer(player)});
      }
      if(room.vote?.active){send(ws,{type:'vote-start',voteId:room.vote.id,candidates:room.vote.candidates});}
      if(room.tiebreak?.active&&room.tiebreak.participants.includes(ws.id)){send(ws,{type:'tiebreak-start',tiebreakId:room.tiebreak.id});}
      if(room.buzzer?.active&&room.buzzer.participants.includes(ws.id)){send(ws,{type:'buzzer-start'});}
      if(room.imageQuestion?.active&&room.imageQuestion.participants.includes(ws.id)){
        send(ws,{type:'image-question-start',id:room.imageQuestion.id,mode:room.imageQuestion.mode,url:room.imageQuestion.url,question:room.imageQuestion.question,color:player.color});
      }
      return;
    }

    if(ws.role==='player'){
      if(['offer','answer','ice'].includes(m.type)){
        send(room.host,{...m,playerId:ws.id});
        send(ws,{type:'signal-server-ack',signal:m.type,direction:'player-to-host'});
        return;
      }
      if(m.type==='client-signal-status'){
        send(room.host,{type:'client-signal-status',playerId:ws.id,stage:m.stage,message:m.message||''});
        return;
      }
      if(m.type==='ready-toggle'){
        const p=room.players.get(ws.id);if(!p)return;
        p.ready=!!m.ready;send(room.host,{type:'player-ready',playerId:p.id,ready:p.ready});send(ws,{type:'ready-state',ready:p.ready});return;
      }
      if(m.type==='buzz'&&room.buzzer?.active&&room.buzzer.participants.includes(ws.id)&&!room.buzzer.winnerId){
        room.buzzer.winnerId=ws.id;
        const p=room.players.get(ws.id);
        send(room.host,{type:'buzzer-winner',playerId:ws.id,name:p?.name||'Spieler'});
        broadcastPlayers(room,{type:'buzzer-result',playerId:ws.id,name:p?.name||'Spieler',color:p?.color||'#4e7cff'});
        return;
      }
      if(m.type==='image-ready'&&room.imageQuestion?.active&&m.id===room.imageQuestion.id){
        room.imageQuestion.ready.add(ws.id);send(room.host,{type:'image-ready-progress',ready:room.imageQuestion.ready.size,total:room.imageQuestion.participants.length});return;
      }
      if(m.type==='image-pin'&&room.imageQuestion?.active&&room.imageQuestion.mode==='pin'&&m.id===room.imageQuestion.id&&!room.imageQuestion.locked){
        const x=Math.max(0,Math.min(1,Number(m.x))),y=Math.max(0,Math.min(1,Number(m.y)));
        room.imageQuestion.answers.set(ws.id,{x,y});send(ws,{type:'image-answer-accepted'});send(room.host,{type:'image-answer-progress',submitted:room.imageQuestion.answers.size,total:room.imageQuestion.participants.length});return;
      }
      if(m.type==='image-text'&&room.imageQuestion?.active&&room.imageQuestion.mode==='text'&&m.id===room.imageQuestion.id&&!room.imageQuestion.locked){
        const text=String(m.text||'').trim().slice(0,120);if(!text)return;
        room.imageQuestion.answers.set(ws.id,{text});send(ws,{type:'image-answer-accepted'});send(room.host,{type:'image-answer-progress',submitted:room.imageQuestion.answers.size,total:room.imageQuestion.participants.length});return;
      }
      if(m.type==='submit-estimate'&&room.estimate?.active&&m.estimateId===room.estimate.id&&room.estimate.participants.includes(ws.id)){
        const value=String(m.value??'').trim().slice(0,30);if(!value)return;
        room.estimate.values.set(ws.id,value);send(ws,{type:'estimate-accepted'});
        send(room.host,{type:'estimate-progress',submitted:room.estimate.values.size,total:room.estimate.participants.length});return;
      }
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
        const p=room.players.get(m.playerId);
        const delivered=!!(p&&p.ws&&p.ws.readyState===WebSocket.OPEN);
        if(delivered)send(p.ws,{...m,from:'host'});
        send(ws,{type:'signal-delivery-ack',signal:m.type,playerId:m.playerId,delivered});
        return;
      }
      if(m.type==='state'){
        const p=room.players.get(m.playerId);if(!p)return;
        if(typeof m.name==='string')p.name=m.name.trim().slice(0,30)||p.name;
        if(Number.isFinite(m.lives))p.lives=Math.max(0,Math.min(99,m.lives));
        if(typeof m.safe==='boolean')p.safe=m.safe;if(typeof m.color==='string')p.color=m.color;
        send(p.ws,{type:'state',name:p.name,lives:p.lives,safe:p.safe,color:p.color});return;
      }
      if(m.type==='player-answer'){
        const p=room.players.get(m.playerId);if(!p)return;
        const counts={
          right:Math.max(0,Number(m.counts?.right)||0),
          wrong:Math.max(0,Number(m.counts?.wrong)||0)
        };
        send(p.ws,{type:'answer-status',counts,history:Array.isArray(m.history)?m.history:[]});
        return;
      }
      if(m.type==='reset-answer-status'){
        broadcastPlayers(room,{type:'answer-status',counts:{right:0,wrong:0}});
        return;
      }
      if(m.type==='player-turn'){const p=room.players.get(m.playerId);if(p)send(p.ws,{type:'player-turn',active:!!m.active});return;}
      if(m.type==='timer-update'){broadcastPlayers(room,{type:'timer-update',left:Number(m.left)||0,initial:Number(m.initial)||0,running:!!m.running});return;}
      if(m.type==='start-estimate'){
        const ids=(m.playerIds||[]).filter(id=>room.players.has(id)&&room.players.get(id).lives>0);
        const er={id:crypto.randomUUID(),active:true,participants:ids,values:new Map()};room.estimate=er;
        for(const id of ids)send(room.players.get(id).ws,{type:'estimate-start',estimateId:er.id});
        send(ws,{type:'estimate-started',estimateId:er.id,total:ids.length});return;
      }
      if(m.type==='reveal-estimate'&&room.estimate?.active){
        room.estimate.active=false;const values=[];
        for(const id of room.estimate.participants)values.push({playerId:id,value:room.estimate.values.get(id)??'—'});
        for(const id of room.estimate.participants)send(room.players.get(id).ws,{type:'estimate-ended'});
        send(ws,{type:'estimate-result',values});return;
      }

      if(m.type==='set-scene'){room.scene=String(m.scene||'main');broadcastPlayers(room,{type:'scene',scene:room.scene});return;}
      if(m.type==='show-reset'){
        room.scene='main';
        if(room.vote)room.vote.active=false;if(room.tiebreak)room.tiebreak.active=false;if(room.estimate)room.estimate.active=false;if(room.buzzer)room.buzzer.active=false;if(room.imageQuestion)room.imageQuestion.active=false;
        broadcastPlayers(room,{type:'show-reset'});return;
      }
      if(m.type==='start-buzzer'){
        const ids=(m.playerIds||[]).filter(id=>room.players.has(id)&&room.players.get(id).lives>0);
        room.buzzer={active:true,participants:ids,winnerId:null};room.scene='buzzer';
        for(const id of ids)send(room.players.get(id).ws,{type:'buzzer-start'});
        send(ws,{type:'buzzer-started',total:ids.length});return;
      }
      if(m.type==='reset-buzzer'&&room.buzzer){
        room.buzzer.winnerId=null;
        for(const id of room.buzzer.participants)send(room.players.get(id)?.ws,{type:'buzzer-start'});
        broadcastPlayers(room,{type:'buzzer-reset'});return;
      }
      if(m.type==='end-buzzer'){
        if(room.buzzer)room.buzzer.active=false;broadcastPlayers(room,{type:'buzzer-end'});return;
      }
      if(m.type==='start-image-question'){
        const ids=(m.playerIds||[]).filter(id=>room.players.has(id)&&room.players.get(id).lives>0);
        room.imageQuestion={id:crypto.randomUUID(),active:true,locked:false,participants:ids,mode:m.mode==='pin'?'pin':'text',url:String(m.url||''),question:String(m.question||'').slice(0,160),target:m.target||null,answers:new Map(),ready:new Set()};
        room.scene='image';
        for(const id of ids){const p=room.players.get(id);send(p.ws,{type:'image-question-start',id:room.imageQuestion.id,mode:room.imageQuestion.mode,url:room.imageQuestion.url,question:room.imageQuestion.question,color:p.color});}
        send(ws,{type:'image-question-started',id:room.imageQuestion.id,total:ids.length});return;
      }
      if(m.type==='lock-image-question'&&room.imageQuestion?.active){
        room.imageQuestion.locked=true;broadcastPlayers(room,{type:'image-question-locked'});return;
      }
      if(m.type==='reveal-image-question'&&room.imageQuestion?.active){
        const q=room.imageQuestion;const values=[];
        for(const id of q.participants){const p=room.players.get(id);values.push({playerId:id,name:p?.name||'Spieler',color:p?.color||'#4e7cff',answer:q.answers.get(id)||null});}
        let nearest=null,farthest=null;
        if(q.mode==='pin'&&q.target&&Number.isFinite(q.target.x)&&Number.isFinite(q.target.y)){
          for(const v of values){if(!v.answer)continue;v.distance=Math.hypot(v.answer.x-q.target.x,v.answer.y-q.target.y);}
          const valid=values.filter(v=>Number.isFinite(v.distance)).sort((a,b)=>a.distance-b.distance);
          nearest=valid[0]?.playerId||null;farthest=valid.at(-1)?.playerId||null;
        }
        send(ws,{type:'image-question-result',id:q.id,mode:q.mode,url:q.url,question:q.question,target:q.target,values,nearest,farthest});return;
      }
      if(m.type==='end-image-question'){
        if(room.imageQuestion)room.imageQuestion.active=false;broadcastPlayers(room,{type:'image-question-end'});return;
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
      if(m.type==='end-tiebreak'){
        if(room.tiebreak){room.tiebreak.active=false;for(const id of room.tiebreak.participants||[])send(room.players.get(id)?.ws,{type:'tiebreak-ended'});}
        return;
      }
      if(m.type==='clear-estimate'){
        if(room.estimate){room.estimate.active=false;for(const id of room.estimate.participants||[])send(room.players.get(id)?.ws,{type:'estimate-ended'});}
        return;
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
